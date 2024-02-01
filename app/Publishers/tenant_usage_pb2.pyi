from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class TenantUsage(_message.Message):
    __slots__ = ["datetime", "dev_eui", "tenant_id", "application_id", "dc_used"]
    DATETIME_FIELD_NUMBER: _ClassVar[int]
    DEV_EUI_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    APPLICATION_ID_FIELD_NUMBER: _ClassVar[int]
    DC_USED_FIELD_NUMBER: _ClassVar[int]
    datetime: str
    dev_eui: str
    tenant_id: str
    application_id: str
    dc_used: int
    def __init__(self, datetime: _Optional[str] = ..., dev_eui: _Optional[str] = ..., tenant_id: _Optional[str] = ..., application_id: _Optional[str] = ..., dc_used: _Optional[int] = ...) -> None: ...
