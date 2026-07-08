import re
from pydantic import (
    AliasChoices,
    BaseModel,
    UUID4,
    Field,
    field_validator,
)
from typing import (
    Optional,
    Dict,
)
from typing_extensions import (
    TypedDict,
)


# Regex to check for valid hexadecimal number
HEXCHECK = re.compile(r'^[0-9a-fA-F]+$')


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# CHIRPSTACK gRPC v4.15.0
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
class GetVariables(TypedDict, total=False):
    max_copies: int = Field(default=0)
    private: bool = Field(default=False)


class GetTags(TypedDict, total=False):
    max_copies: int = Field(default=0)
    private: bool = Field(default=False)


class GetDeviceSyncRequest(BaseModel):
    # # get device
    devEui: str
    name: str
    isDisabled: bool
    variables: GetVariables[Dict]
    tags: GetTags[Dict]
    joinEui: str
    # # deviceActivated device portion
    devAddr: Optional[str] = Field(default=0)
    appSKey: Optional[str] = Field(default=0)
    nwkSEncKey: Optional[str] = Field(default=0)
    # # deviceKeys portion
    nwkKey: Optional[str] = Field(default=0)

    @field_validator('devEui', 'joinEui', 'devAddr')
    @classmethod
    def eui_hex_value_check(cls, v):
        """convert hexidecimal value to integer for helium rpc"""
        if HEXCHECK.match(v) is None:
            raise ValueError(f'hex value required got: {v}')
        return int(v, 16)

    @property
    def is_private(self) -> bool:
        """device private flag, tags take precedence over variables when both are set"""
        return self.tags['private'] if 'private' in self.tags else self.variables.get('private', False)

    @property
    def max_copies(self) -> int:
        """device max_copies, tags take precedence over variables when both are set"""
        return self.tags['max_copies'] if 'max_copies' in self.tags else self.variables.get('max_copies', 0)


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
    @classmethod
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
    @classmethod
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
    routeId: str
    joinEui: int = Field(validation_alias=AliasChoices('appEui'))
    devEui: int

    @field_validator('joinEui', 'devEui')
    @classmethod
    def hex_value_check(cls, to_hex):
        """convert integer value to hexidecimal"""
        return hex(to_hex)[2:]


class GetRouteSkfsList(BaseModel):
    routeId: str
    devaddr: int
    sessionKey: str
    maxCopies: Optional[int] = Field(default=0)
