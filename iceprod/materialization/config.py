import dataclasses

from wipac_dev_tools import from_environment_as_dataclass


@dataclasses.dataclass
class DefaultConfig:
    HOST: str = 'localhost'
    PORT: int = 8080
    DEBUG: bool = False
    OPENID_URL: str = ''
    OPENID_AUDIENCE: str = ''
    ICEPROD_API_ADDRESS: str = 'https://iceprod2-api.icecube.wisc.edu'
    ICEPROD_API_CLIENT_ID: str = ''
    ICEPROD_API_CLIENT_SECRET: str = ''
    DB_URL: str = 'mongodb://localhost/iceprod'
    DB_TIMEOUT: int = 60
    DB_WRITE_CONCERN: int = 1
    PROMETHEUS_PORT: int = 0
    CI_TESTING: str = ''


def get_config() -> DefaultConfig:
    return from_environment_as_dataclass(DefaultConfig)
