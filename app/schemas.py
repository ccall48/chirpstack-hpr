import re
# from uuid import UUID
from pydantic import (
    AliasChoices,
    # Base64Str,
    BaseModel,
    # ConfigDict,
    # UUID1,
    UUID4,
    Field,
    # Json,
    # constr,
    # ValidationError,
    field_validator,
)
from typing import (
    # Annotated,
    Optional,
    # List,
    Dict,
)
from typing_extensions import (
    TypedDict,
    # NotRequired,
)


HEXCHECK = re.compile(r'^[0-9a-fA-F]+$')


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# CHIRPSTACK gRPC v4.11.0
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
class GetVariables(TypedDict, total=False):
    max_copies: int = Field(default=0)
    private: bool = Field(default=False)


class GetDeviceSyncRequest(BaseModel):
    # # get device
    devEui: str
    name: str
    isDisabled: bool
    variables: GetVariables[Dict]
    tags: Optional[Dict[str, str]] = {}
    joinEui: str
    # # deviceActivated device portion
    devAddr: Optional[str] = Field(default=0)
    appSKey: Optional[str] = Field(default=0)
    nwkSEncKey: Optional[str] = Field(default=0)
    # # deviceKeys portion
    nwkKey: Optional[str] = Field(default=0)

    @field_validator('devEui', 'joinEui', 'devAddr')
    def eui_hex_value_check(cls, v):
        """convert hexidecimal value to integer for helium rpc"""
        if HEXCHECK.match(v) is None:
            raise ValueError(f'hex value required got: {v}')
        return int(v, 16)


class GetDeviceRequest(BaseModel):
    devEui: str
    name: str
    applicationId: UUID4
    deviceProfileId: UUID4
    isDisabled: bool
    variables: Optional[Dict[str, str]] = {}
    tags: Optional[Dict[str, str]] = {}
    joinEui: str

    @field_validator('devEui', 'joinEui')
    def eui_hex_value_check(cls, v):
        """convert hexidecimal value to integer"""
        if HEXCHECK.match(v) is None:
            raise ValueError(f'hex value required devEui/joinEui got: {v}')
        return int(v, 16)


class GetDeviceActivation(BaseModel):
    devEui: str
    devAddr: str
    appSKey: str
    nwkSEncKey: str

    @field_validator('devEui', 'devAddr')
    def eui_hex_value_check(cls, v):
        """convert hexidecimal value to integer"""
        if HEXCHECK.match(v) is None:
            raise ValueError(f'hex value required devEui/devAddr got: {v}')
        return int(v, 16)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# HELIUM gRPC
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
class GetRouteEuisList(BaseModel):
    """appEui == joinEui"""
    routeId: str  # UUID
    joinEui: int = Field(validation_alias=AliasChoices('appEui'))
    devEui: int

    @field_validator('joinEui', 'devEui')
    def hex_value_check(cls, to_hex):
        """convert integer value to hexidecimal"""
        return hex(to_hex)[2:]


class GetRouteSkfsList(BaseModel):
    routeId: str  # UUID
    devaddr: int
    sessionKey: str
    maxCopies: Optional[int] = Field(default=0)

    # @field_validator('devaddr')
    # def int_to_hex(cls, v):
    #     return hex(v)[2:]

    # @field_validator('sessionKey')
    # def hex_value_check(cls, v):
    #     if HEXCHECK.match(v) is None:
    #         raise ValueError(f'hex value required for sessionKey got: {v}')
    #     return v
