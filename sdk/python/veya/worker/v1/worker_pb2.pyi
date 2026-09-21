from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ProtocolVersion(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PROTOCOL_VERSION_UNSPECIFIED: _ClassVar[ProtocolVersion]
    PROTOCOL_VERSION_1: _ClassVar[ProtocolVersion]

class EffectClass(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    EFFECT_CLASS_UNSPECIFIED: _ClassVar[EffectClass]
    EFFECT_CLASS_NONE: _ClassVar[EffectClass]
    EFFECT_CLASS_IDEMPOTENT_BY_KEY: _ClassVar[EffectClass]
    EFFECT_CLASS_QUERYABLE: _ClassVar[EffectClass]
    EFFECT_CLASS_UNRECONCILABLE: _ClassVar[EffectClass]

class JoinKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    JOIN_KIND_UNSPECIFIED: _ClassVar[JoinKind]
    JOIN_KIND_ALL: _ClassVar[JoinKind]
    JOIN_KIND_ANY: _ClassVar[JoinKind]
    JOIN_KIND_QUORUM: _ClassVar[JoinKind]

class DecisionKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    DECISION_KIND_UNSPECIFIED: _ClassVar[DecisionKind]
    DECISION_KIND_CALL_TOOL: _ClassVar[DecisionKind]
    DECISION_KIND_COMPLETE: _ClassVar[DecisionKind]
    DECISION_KIND_FAIL: _ClassVar[DecisionKind]
    DECISION_KIND_CALL_TOOL_PARALLEL: _ClassVar[DecisionKind]
    DECISION_KIND_SLEEP: _ClassVar[DecisionKind]
    DECISION_KIND_WAIT_FOR_SIGNAL: _ClassVar[DecisionKind]
    DECISION_KIND_CANCEL: _ClassVar[DecisionKind]
    DECISION_KIND_COMPENSATE: _ClassVar[DecisionKind]

class ResolutionKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    RESOLUTION_KIND_UNSPECIFIED: _ClassVar[ResolutionKind]
    RESOLUTION_KIND_COMMITTED: _ClassVar[ResolutionKind]
    RESOLUTION_KIND_NOT_EXECUTED: _ClassVar[ResolutionKind]
    RESOLUTION_KIND_STILL_UNKNOWN: _ClassVar[ResolutionKind]

class Certainty(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CERTAINTY_UNSPECIFIED: _ClassVar[Certainty]
    CERTAINTY_NOT_EXECUTED: _ClassVar[Certainty]
    CERTAINTY_UNKNOWN: _ClassVar[Certainty]
PROTOCOL_VERSION_UNSPECIFIED: ProtocolVersion
PROTOCOL_VERSION_1: ProtocolVersion
EFFECT_CLASS_UNSPECIFIED: EffectClass
EFFECT_CLASS_NONE: EffectClass
EFFECT_CLASS_IDEMPOTENT_BY_KEY: EffectClass
EFFECT_CLASS_QUERYABLE: EffectClass
EFFECT_CLASS_UNRECONCILABLE: EffectClass
JOIN_KIND_UNSPECIFIED: JoinKind
JOIN_KIND_ALL: JoinKind
JOIN_KIND_ANY: JoinKind
JOIN_KIND_QUORUM: JoinKind
DECISION_KIND_UNSPECIFIED: DecisionKind
DECISION_KIND_CALL_TOOL: DecisionKind
DECISION_KIND_COMPLETE: DecisionKind
DECISION_KIND_FAIL: DecisionKind
DECISION_KIND_CALL_TOOL_PARALLEL: DecisionKind
DECISION_KIND_SLEEP: DecisionKind
DECISION_KIND_WAIT_FOR_SIGNAL: DecisionKind
DECISION_KIND_CANCEL: DecisionKind
DECISION_KIND_COMPENSATE: DecisionKind
RESOLUTION_KIND_UNSPECIFIED: ResolutionKind
RESOLUTION_KIND_COMMITTED: ResolutionKind
RESOLUTION_KIND_NOT_EXECUTED: ResolutionKind
RESOLUTION_KIND_STILL_UNKNOWN: ResolutionKind
CERTAINTY_UNSPECIFIED: Certainty
CERTAINTY_NOT_EXECUTED: Certainty
CERTAINTY_UNKNOWN: Certainty

class ClientMessage(_message.Message):
    __slots__ = ("register", "decide_result", "execute_result", "reconcile_result")
    REGISTER_FIELD_NUMBER: _ClassVar[int]
    DECIDE_RESULT_FIELD_NUMBER: _ClassVar[int]
    EXECUTE_RESULT_FIELD_NUMBER: _ClassVar[int]
    RECONCILE_RESULT_FIELD_NUMBER: _ClassVar[int]
    register: Register
    decide_result: DecideResult
    execute_result: ExecuteResult
    reconcile_result: ReconcileResult
    def __init__(self, register: _Optional[_Union[Register, _Mapping]] = ..., decide_result: _Optional[_Union[DecideResult, _Mapping]] = ..., execute_result: _Optional[_Union[ExecuteResult, _Mapping]] = ..., reconcile_result: _Optional[_Union[ReconcileResult, _Mapping]] = ...) -> None: ...

class ServerMessage(_message.Message):
    __slots__ = ("registered", "decide", "execute", "reconcile")
    REGISTERED_FIELD_NUMBER: _ClassVar[int]
    DECIDE_FIELD_NUMBER: _ClassVar[int]
    EXECUTE_FIELD_NUMBER: _ClassVar[int]
    RECONCILE_FIELD_NUMBER: _ClassVar[int]
    registered: Registered
    decide: DecideRequest
    execute: ExecuteRequest
    reconcile: ReconcileRequest
    def __init__(self, registered: _Optional[_Union[Registered, _Mapping]] = ..., decide: _Optional[_Union[DecideRequest, _Mapping]] = ..., execute: _Optional[_Union[ExecuteRequest, _Mapping]] = ..., reconcile: _Optional[_Union[ReconcileRequest, _Mapping]] = ...) -> None: ...

class Register(_message.Message):
    __slots__ = ("protocol_version", "agent_name", "agent_version", "worker_id", "runtime", "tools", "decides")
    PROTOCOL_VERSION_FIELD_NUMBER: _ClassVar[int]
    AGENT_NAME_FIELD_NUMBER: _ClassVar[int]
    AGENT_VERSION_FIELD_NUMBER: _ClassVar[int]
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    RUNTIME_FIELD_NUMBER: _ClassVar[int]
    TOOLS_FIELD_NUMBER: _ClassVar[int]
    DECIDES_FIELD_NUMBER: _ClassVar[int]
    protocol_version: ProtocolVersion
    agent_name: str
    agent_version: str
    worker_id: str
    runtime: str
    tools: _containers.RepeatedCompositeFieldContainer[ToolDescriptor]
    decides: bool
    def __init__(self, protocol_version: _Optional[_Union[ProtocolVersion, str]] = ..., agent_name: _Optional[str] = ..., agent_version: _Optional[str] = ..., worker_id: _Optional[str] = ..., runtime: _Optional[str] = ..., tools: _Optional[_Iterable[_Union[ToolDescriptor, _Mapping]]] = ..., decides: _Optional[bool] = ...) -> None: ...

class Registered(_message.Message):
    __slots__ = ("session_id", "runtime_version")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    RUNTIME_VERSION_FIELD_NUMBER: _ClassVar[int]
    session_id: str
    runtime_version: str
    def __init__(self, session_id: _Optional[str] = ..., runtime_version: _Optional[str] = ...) -> None: ...

class ToolDescriptor(_message.Message):
    __slots__ = ("name", "effect_class", "key_ttl_seconds", "reconcilable")
    NAME_FIELD_NUMBER: _ClassVar[int]
    EFFECT_CLASS_FIELD_NUMBER: _ClassVar[int]
    KEY_TTL_SECONDS_FIELD_NUMBER: _ClassVar[int]
    RECONCILABLE_FIELD_NUMBER: _ClassVar[int]
    name: str
    effect_class: EffectClass
    key_ttl_seconds: int
    reconcilable: bool
    def __init__(self, name: _Optional[str] = ..., effect_class: _Optional[_Union[EffectClass, str]] = ..., key_ttl_seconds: _Optional[int] = ..., reconcilable: _Optional[bool] = ...) -> None: ...

class DecideRequest(_message.Message):
    __slots__ = ("call_id", "run", "history")
    CALL_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_FIELD_NUMBER: _ClassVar[int]
    HISTORY_FIELD_NUMBER: _ClassVar[int]
    call_id: str
    run: Run
    history: _containers.RepeatedCompositeFieldContainer[Event]
    def __init__(self, call_id: _Optional[str] = ..., run: _Optional[_Union[Run, _Mapping]] = ..., history: _Optional[_Iterable[_Union[Event, _Mapping]]] = ...) -> None: ...

class DecideResult(_message.Message):
    __slots__ = ("call_id", "decision", "failure")
    CALL_ID_FIELD_NUMBER: _ClassVar[int]
    DECISION_FIELD_NUMBER: _ClassVar[int]
    FAILURE_FIELD_NUMBER: _ClassVar[int]
    call_id: str
    decision: Decision
    failure: Failure
    def __init__(self, call_id: _Optional[str] = ..., decision: _Optional[_Union[Decision, _Mapping]] = ..., failure: _Optional[_Union[Failure, _Mapping]] = ...) -> None: ...

class Decision(_message.Message):
    __slots__ = ("kind", "step_id", "tool", "payload", "output", "error", "calls", "join", "wake_at_unix_nano", "signal", "cancel_reason")
    KIND_FIELD_NUMBER: _ClassVar[int]
    STEP_ID_FIELD_NUMBER: _ClassVar[int]
    TOOL_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    CALLS_FIELD_NUMBER: _ClassVar[int]
    JOIN_FIELD_NUMBER: _ClassVar[int]
    WAKE_AT_UNIX_NANO_FIELD_NUMBER: _ClassVar[int]
    SIGNAL_FIELD_NUMBER: _ClassVar[int]
    CANCEL_REASON_FIELD_NUMBER: _ClassVar[int]
    kind: DecisionKind
    step_id: str
    tool: str
    payload: bytes
    output: bytes
    error: str
    calls: _containers.RepeatedCompositeFieldContainer[ToolCall]
    join: JoinPolicy
    wake_at_unix_nano: int
    signal: SignalWait
    cancel_reason: str
    def __init__(self, kind: _Optional[_Union[DecisionKind, str]] = ..., step_id: _Optional[str] = ..., tool: _Optional[str] = ..., payload: _Optional[bytes] = ..., output: _Optional[bytes] = ..., error: _Optional[str] = ..., calls: _Optional[_Iterable[_Union[ToolCall, _Mapping]]] = ..., join: _Optional[_Union[JoinPolicy, _Mapping]] = ..., wake_at_unix_nano: _Optional[int] = ..., signal: _Optional[_Union[SignalWait, _Mapping]] = ..., cancel_reason: _Optional[str] = ...) -> None: ...

class ToolCall(_message.Message):
    __slots__ = ("tool", "payload")
    TOOL_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    tool: str
    payload: bytes
    def __init__(self, tool: _Optional[str] = ..., payload: _Optional[bytes] = ...) -> None: ...

class JoinPolicy(_message.Message):
    __slots__ = ("kind", "quorum")
    KIND_FIELD_NUMBER: _ClassVar[int]
    QUORUM_FIELD_NUMBER: _ClassVar[int]
    kind: JoinKind
    quorum: int
    def __init__(self, kind: _Optional[_Union[JoinKind, str]] = ..., quorum: _Optional[int] = ...) -> None: ...

class SignalWait(_message.Message):
    __slots__ = ("name", "deadline_unix_nano")
    NAME_FIELD_NUMBER: _ClassVar[int]
    DEADLINE_UNIX_NANO_FIELD_NUMBER: _ClassVar[int]
    name: str
    deadline_unix_nano: int
    def __init__(self, name: _Optional[str] = ..., deadline_unix_nano: _Optional[int] = ...) -> None: ...

class Run(_message.Message):
    __slots__ = ("run_id", "agent_name", "agent_version", "status", "input")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    AGENT_NAME_FIELD_NUMBER: _ClassVar[int]
    AGENT_VERSION_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    agent_name: str
    agent_version: str
    status: str
    input: bytes
    def __init__(self, run_id: _Optional[str] = ..., agent_name: _Optional[str] = ..., agent_version: _Optional[str] = ..., status: _Optional[str] = ..., input: _Optional[bytes] = ...) -> None: ...

class Event(_message.Message):
    __slots__ = ("run_id", "seq", "type", "step_id", "payload", "created_at_unix_nano")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    SEQ_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    STEP_ID_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_UNIX_NANO_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    seq: int
    type: str
    step_id: str
    payload: bytes
    created_at_unix_nano: int
    def __init__(self, run_id: _Optional[str] = ..., seq: _Optional[int] = ..., type: _Optional[str] = ..., step_id: _Optional[str] = ..., payload: _Optional[bytes] = ..., created_at_unix_nano: _Optional[int] = ...) -> None: ...

class ExecuteRequest(_message.Message):
    __slots__ = ("call_id", "tool", "run_id", "task_id", "step_id", "idempotency_key", "payload", "attempt")
    CALL_ID_FIELD_NUMBER: _ClassVar[int]
    TOOL_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    STEP_ID_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    call_id: str
    tool: str
    run_id: str
    task_id: str
    step_id: str
    idempotency_key: str
    payload: bytes
    attempt: int
    def __init__(self, call_id: _Optional[str] = ..., tool: _Optional[str] = ..., run_id: _Optional[str] = ..., task_id: _Optional[str] = ..., step_id: _Optional[str] = ..., idempotency_key: _Optional[str] = ..., payload: _Optional[bytes] = ..., attempt: _Optional[int] = ...) -> None: ...

class ExecuteResult(_message.Message):
    __slots__ = ("call_id", "result", "failure")
    CALL_ID_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    FAILURE_FIELD_NUMBER: _ClassVar[int]
    call_id: str
    result: bytes
    failure: Failure
    def __init__(self, call_id: _Optional[str] = ..., result: _Optional[bytes] = ..., failure: _Optional[_Union[Failure, _Mapping]] = ...) -> None: ...

class ReconcileRequest(_message.Message):
    __slots__ = ("call_id", "tool", "effect")
    CALL_ID_FIELD_NUMBER: _ClassVar[int]
    TOOL_FIELD_NUMBER: _ClassVar[int]
    EFFECT_FIELD_NUMBER: _ClassVar[int]
    call_id: str
    tool: str
    effect: Effect
    def __init__(self, call_id: _Optional[str] = ..., tool: _Optional[str] = ..., effect: _Optional[_Union[Effect, _Mapping]] = ...) -> None: ...

class ReconcileResult(_message.Message):
    __slots__ = ("call_id", "resolution", "failure")
    CALL_ID_FIELD_NUMBER: _ClassVar[int]
    RESOLUTION_FIELD_NUMBER: _ClassVar[int]
    FAILURE_FIELD_NUMBER: _ClassVar[int]
    call_id: str
    resolution: Resolution
    failure: Failure
    def __init__(self, call_id: _Optional[str] = ..., resolution: _Optional[_Union[Resolution, _Mapping]] = ..., failure: _Optional[_Union[Failure, _Mapping]] = ...) -> None: ...

class Resolution(_message.Message):
    __slots__ = ("kind", "response", "external_ref", "detail")
    KIND_FIELD_NUMBER: _ClassVar[int]
    RESPONSE_FIELD_NUMBER: _ClassVar[int]
    EXTERNAL_REF_FIELD_NUMBER: _ClassVar[int]
    DETAIL_FIELD_NUMBER: _ClassVar[int]
    kind: ResolutionKind
    response: bytes
    external_ref: str
    detail: str
    def __init__(self, kind: _Optional[_Union[ResolutionKind, str]] = ..., response: _Optional[bytes] = ..., external_ref: _Optional[str] = ..., detail: _Optional[str] = ...) -> None: ...

class Effect(_message.Message):
    __slots__ = ("effect_id", "run_id", "task_id", "effect_type", "effect_class", "idempotency_key", "status", "external_ref", "request", "response", "created_at_unix_nano")
    EFFECT_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    EFFECT_TYPE_FIELD_NUMBER: _ClassVar[int]
    EFFECT_CLASS_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    EXTERNAL_REF_FIELD_NUMBER: _ClassVar[int]
    REQUEST_FIELD_NUMBER: _ClassVar[int]
    RESPONSE_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_UNIX_NANO_FIELD_NUMBER: _ClassVar[int]
    effect_id: str
    run_id: str
    task_id: str
    effect_type: str
    effect_class: EffectClass
    idempotency_key: str
    status: str
    external_ref: str
    request: bytes
    response: bytes
    created_at_unix_nano: int
    def __init__(self, effect_id: _Optional[str] = ..., run_id: _Optional[str] = ..., task_id: _Optional[str] = ..., effect_type: _Optional[str] = ..., effect_class: _Optional[_Union[EffectClass, str]] = ..., idempotency_key: _Optional[str] = ..., status: _Optional[str] = ..., external_ref: _Optional[str] = ..., request: _Optional[bytes] = ..., response: _Optional[bytes] = ..., created_at_unix_nano: _Optional[int] = ...) -> None: ...

class Failure(_message.Message):
    __slots__ = ("message", "type", "certainty")
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    CERTAINTY_FIELD_NUMBER: _ClassVar[int]
    message: str
    type: str
    certainty: Certainty
    def __init__(self, message: _Optional[str] = ..., type: _Optional[str] = ..., certainty: _Optional[_Union[Certainty, str]] = ...) -> None: ...
