// Package jetstream carries task deliveries over NATS JetStream.
//
// It is the adapter that lets workers live in another process, on another
// machine, without polling the database. Everything durable still lives in
// PostgreSQL; this moves nothing but identities.
//
// # What is and is not trusted here
//
// A message says "task T may need attention". That is the entire content. The
// receiver reads what is actually true from the store, so a message that is
// duplicated, delayed, reordered, or delivered to two workers at once carries
// nothing that could be wrong. The stream is not a source of truth and is not
// treated as one — if every message vanished, the recovery scan would find the
// same work in PostgreSQL and republish it.
//
// # Acknowledgement, and why it is immediate
//
// A message is acked as soon as it is decoded, before the worker has claimed
// anything. That looks careless and is a deliberate reading of what redelivery
// can actually buy here.
//
// JetStream would redeliver an unacked message after AckWait. But the broker
// cannot tell whether the task was claimed — only PostgreSQL knows that — so
// its redelivery is a guess, and a correct guess is indistinguishable from the
// recovery scan finding the same PENDING task a moment later. Holding the ack
// across the whole tool call would additionally mean either threading
// acknowledgement through core.Dispatcher, which exists to carry identity and
// nothing else, or renewing an ack deadline in parallel with the lease — a
// second liveness mechanism saying the same thing as the first, free to
// disagree with it.
//
// The cost is real and bounded: if this process dies between the ack and the
// claim, the task waits for the next recovery scan instead of being redelivered
// immediately. That is a latency cost on a path that only runs after a crash,
// which is the kind of cost this design accepts everywhere else.
package jetstream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/SanthoshRaaj-KR/Veya/internal/core"
)

// Defaults for a local `make up` environment.
const (
	DefaultURL     = "nats://127.0.0.1:4222"
	DefaultStream  = "VEYA_TASKS"
	DefaultDurable = "veya-workers"
)

// Dispatcher is a core.Dispatcher backed by a JetStream work queue.
type Dispatcher struct {
	conn     *nats.Conn
	js       jetstream.JetStream
	consumer jetstream.Consumer
	subject  string

	// fetchWait bounds one blocking Fetch. It is not a timeout on Claim: Claim
	// keeps fetching until it has a task or its context ends. A short wait just
	// means the loop notices a Close sooner.
	fetchWait time.Duration

	log    *slog.Logger
	closed chan struct{}
	once   sync.Once
}

// Config wires a Dispatcher. Every field has a working default.
type Config struct {
	URL     string // NATS server, e.g. nats://127.0.0.1:4222
	Stream  string // stream name
	Durable string // durable consumer name, shared by every worker
	Subject string // defaults to core.DeliverySubject

	// MaxAge is how long an undelivered message survives in the stream.
	//
	// Losing one is survivable — the task is still PENDING in PostgreSQL and
	// the recovery scan republishes it — so this exists to stop an unreachable
	// consumer filling the disk, not to protect the work.
	MaxAge time.Duration

	FetchWait time.Duration
	Logger    *slog.Logger
}

// Open connects to NATS and makes sure the stream and consumer exist.
//
// Both are created if missing and updated if present, so a fresh environment
// and a redeploy take the same path. Nothing here is a migration: the stream
// holds no durable state worth preserving.
func Open(ctx context.Context, cfg Config) (*Dispatcher, error) {
	url := cfg.URL
	if url == "" {
		url = DefaultURL
	}
	streamName := cfg.Stream
	if streamName == "" {
		streamName = DefaultStream
	}
	durable := cfg.Durable
	if durable == "" {
		durable = DefaultDurable
	}
	subject := cfg.Subject
	if subject == "" {
		subject = core.DeliverySubject
	}
	maxAge := cfg.MaxAge
	if maxAge <= 0 {
		maxAge = 24 * time.Hour
	}
	fetchWait := cfg.FetchWait
	if fetchWait <= 0 {
		fetchWait = 2 * time.Second
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}

	conn, err := nats.Connect(url,
		nats.Name("veya"),
		// Reconnect forever. A broker outage must not kill a runtime: the relay
		// keeps the undelivered rows, and work resumes when NATS comes back.
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			log.Warn("nats disconnected; deliveries stay in the outbox", "error", err)
		}),
		nats.ReconnectHandler(func(*nats.Conn) { log.Info("nats reconnected") }),
	)
	if err != nil {
		return nil, fmt.Errorf("jetstream: connect %s: %w", url, err)
	}

	js, err := jetstream.New(conn)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("jetstream: open: %w", err)
	}

	stream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     streamName,
		Subjects: []string{subject},
		// A work queue, not a log: a message is removed once acknowledged.
		// The alternative — keeping every delivery forever — would make the
		// stream a second history, competing with the one in PostgreSQL.
		Retention: jetstream.WorkQueuePolicy,
		Storage:   jetstream.FileStorage,
		Discard:   jetstream.DiscardOld,
		MaxAge:    maxAge,
	})
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("jetstream: create stream %s: %w", streamName, err)
	}

	// One durable consumer shared by every worker. Each worker pulls from it,
	// so adding a worker adds capacity rather than adding a copy of the work.
	consumer, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:   durable,
		AckPolicy: jetstream.AckExplicitPolicy,
		AckWait:   30 * time.Second,
	})
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("jetstream: create consumer %s: %w", durable, err)
	}

	log.Info("jetstream dispatcher ready",
		"url", url, "stream", streamName, "durable", durable, "subject", subject)

	return &Dispatcher{
		conn:      conn,
		js:        js,
		consumer:  consumer,
		subject:   subject,
		fetchWait: fetchWait,
		log:       log,
		closed:    make(chan struct{}),
	}, nil
}

// Publish sends one task identity and waits for the server to confirm it.
//
// Waiting matters: the relay marks an outbox row published only when this
// returns nil, so nil has to mean the message is in the stream. A fire-and-
// forget publish would let the relay record a delivery that never happened,
// which is the dual write again wearing a different hat.
func (d *Dispatcher) Publish(ctx context.Context, id core.TaskID) error {
	select {
	case <-d.closed:
		return core.ErrDispatcherClosed
	default:
	}

	payload, err := core.EncodeDelivery(id)
	if err != nil {
		return err
	}
	if _, err := d.js.Publish(ctx, d.subject, payload); err != nil {
		return fmt.Errorf("jetstream: publish %s: %w", id, err)
	}
	return nil
}

// Claim blocks until a task arrives, ctx is done, or the dispatcher closes.
func (d *Dispatcher) Claim(ctx context.Context) (core.TaskID, error) {
	for {
		select {
		case <-d.closed:
			return "", core.ErrDispatcherClosed
		case <-ctx.Done():
			return "", ctx.Err()
		default:
		}

		id, ok, err := d.fetchOne(ctx)
		switch {
		case err != nil:
			return "", err
		case ok:
			return id, nil
		}
		// An empty fetch is the normal idle case, not a failure: there is no
		// work right now. Loop and wait again.
	}
}

// fetchOne pulls at most one message and reports whether it produced a task.
func (d *Dispatcher) fetchOne(ctx context.Context) (core.TaskID, bool, error) {
	batch, err := d.consumer.Fetch(1, jetstream.FetchMaxWait(d.fetchWait))
	if err != nil {
		if errors.Is(err, nats.ErrConnectionClosed) {
			return "", false, core.ErrDispatcherClosed
		}
		return "", false, fmt.Errorf("jetstream: fetch: %w", err)
	}

	for msg := range batch.Messages() {
		id, err := core.DecodeDelivery(msg.Data())
		if err != nil {
			// Unparseable. Terminate rather than let it redeliver forever: a
			// work queue blocked behind a poison message stops every task
			// after it, and the task this refers to — if it refers to one — is
			// still PENDING and will be found by the recovery scan.
			d.log.Error("discarding an unreadable delivery", "error", err)
			if err := msg.Term(); err != nil {
				d.log.Warn("could not terminate an unreadable delivery", "error", err)
			}
			continue
		}

		// Acked before the claim is attempted. See the package comment: the
		// broker cannot know whether the task was claimed, so its redelivery
		// would be a guess, and PostgreSQL already holds the answer.
		if err := msg.Ack(); err != nil {
			// The message will be redelivered after AckWait and rejected by
			// the conditional claim. Costs a duplicate, loses nothing.
			d.log.Warn("could not acknowledge a delivery", "task_id", id, "error", err)
		}
		return id, true, nil
	}

	if err := batch.Error(); err != nil {
		if errors.Is(err, nats.ErrConnectionClosed) {
			return "", false, core.ErrDispatcherClosed
		}
		return "", false, fmt.Errorf("jetstream: fetch batch: %w", err)
	}
	return "", false, nil
}

// Close stops delivery and disconnects. Safe to call more than once.
func (d *Dispatcher) Close() error {
	d.once.Do(func() {
		close(d.closed)
		// Drain rather than Close, so a publish already in flight completes
		// instead of being abandoned half-written.
		if err := d.conn.Drain(); err != nil {
			d.log.Warn("nats drain failed", "error", err)
			d.conn.Close()
		}
	})
	return nil
}
