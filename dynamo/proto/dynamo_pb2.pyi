from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class VectorClockItem(_message.Message):
    __slots__ = ("node_id", "counter")
    NODE_ID_FIELD_NUMBER: _ClassVar[int]
    COUNTER_FIELD_NUMBER: _ClassVar[int]
    node_id: str
    counter: int
    def __init__(self, node_id: _Optional[str] = ..., counter: _Optional[int] = ...) -> None: ...

class Context(_message.Message):
    __slots__ = ("clock",)
    CLOCK_FIELD_NUMBER: _ClassVar[int]
    clock: _containers.RepeatedCompositeFieldContainer[VectorClockItem]
    def __init__(self, clock: _Optional[_Iterable[_Union[VectorClockItem, _Mapping]]] = ...) -> None: ...

class ValueWithContext(_message.Message):
    __slots__ = ("value", "context", "dot")
    VALUE_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    DOT_FIELD_NUMBER: _ClassVar[int]
    value: bytes
    context: Context
    dot: VectorClockItem
    def __init__(self, value: _Optional[bytes] = ..., context: _Optional[_Union[Context, _Mapping]] = ..., dot: _Optional[_Union[VectorClockItem, _Mapping]] = ...) -> None: ...

class NodeState(_message.Message):
    __slots__ = ("node_id", "address", "generation", "last_updated_time", "up")
    NODE_ID_FIELD_NUMBER: _ClassVar[int]
    ADDRESS_FIELD_NUMBER: _ClassVar[int]
    GENERATION_FIELD_NUMBER: _ClassVar[int]
    LAST_UPDATED_TIME_FIELD_NUMBER: _ClassVar[int]
    UP_FIELD_NUMBER: _ClassVar[int]
    node_id: str
    address: str
    generation: int
    last_updated_time: int
    up: bool
    def __init__(self, node_id: _Optional[str] = ..., address: _Optional[str] = ..., generation: _Optional[int] = ..., last_updated_time: _Optional[int] = ..., up: _Optional[bool] = ...) -> None: ...

class MembershipList(_message.Message):
    __slots__ = ("members",)
    MEMBERS_FIELD_NUMBER: _ClassVar[int]
    members: _containers.RepeatedCompositeFieldContainer[NodeState]
    def __init__(self, members: _Optional[_Iterable[_Union[NodeState, _Mapping]]] = ...) -> None: ...

class Hint(_message.Message):
    __slots__ = ("key", "data", "target_node_id", "timestamp_ms")
    KEY_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    TARGET_NODE_ID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_MS_FIELD_NUMBER: _ClassVar[int]
    key: str
    data: ValueWithContext
    target_node_id: str
    timestamp_ms: int
    def __init__(self, key: _Optional[str] = ..., data: _Optional[_Union[ValueWithContext, _Mapping]] = ..., target_node_id: _Optional[str] = ..., timestamp_ms: _Optional[int] = ...) -> None: ...

class DeliverHintsRequest(_message.Message):
    __slots__ = ("target_node_id", "hints")
    TARGET_NODE_ID_FIELD_NUMBER: _ClassVar[int]
    HINTS_FIELD_NUMBER: _ClassVar[int]
    target_node_id: str
    hints: _containers.RepeatedCompositeFieldContainer[Hint]
    def __init__(self, target_node_id: _Optional[str] = ..., hints: _Optional[_Iterable[_Union[Hint, _Mapping]]] = ...) -> None: ...

class DeliverHintsReply(_message.Message):
    __slots__ = ("success", "message")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    success: bool
    message: str
    def __init__(self, success: _Optional[bool] = ..., message: _Optional[str] = ...) -> None: ...

class PutRequest(_message.Message):
    __slots__ = ("key", "data", "client_id", "timestamp_ms")
    KEY_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    CLIENT_ID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_MS_FIELD_NUMBER: _ClassVar[int]
    key: str
    data: ValueWithContext
    client_id: str
    timestamp_ms: int
    def __init__(self, key: _Optional[str] = ..., data: _Optional[_Union[ValueWithContext, _Mapping]] = ..., client_id: _Optional[str] = ..., timestamp_ms: _Optional[int] = ...) -> None: ...

class PutReply(_message.Message):
    __slots__ = ("success", "message")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    success: bool
    message: str
    def __init__(self, success: _Optional[bool] = ..., message: _Optional[str] = ...) -> None: ...

class GetRequest(_message.Message):
    __slots__ = ("key", "client_id")
    KEY_FIELD_NUMBER: _ClassVar[int]
    CLIENT_ID_FIELD_NUMBER: _ClassVar[int]
    key: str
    client_id: str
    def __init__(self, key: _Optional[str] = ..., client_id: _Optional[str] = ...) -> None: ...

class GetReply(_message.Message):
    __slots__ = ("data", "found", "message")
    DATA_FIELD_NUMBER: _ClassVar[int]
    FOUND_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    data: _containers.RepeatedCompositeFieldContainer[ValueWithContext]
    found: bool
    message: str
    def __init__(self, data: _Optional[_Iterable[_Union[ValueWithContext, _Mapping]]] = ..., found: _Optional[bool] = ..., message: _Optional[str] = ...) -> None: ...

class StoredRecord(_message.Message):
    __slots__ = ("key", "versions")
    KEY_FIELD_NUMBER: _ClassVar[int]
    VERSIONS_FIELD_NUMBER: _ClassVar[int]
    key: str
    versions: _containers.RepeatedCompositeFieldContainer[ValueWithContext]
    def __init__(self, key: _Optional[str] = ..., versions: _Optional[_Iterable[_Union[ValueWithContext, _Mapping]]] = ...) -> None: ...
