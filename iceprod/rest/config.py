import dataclasses

from wipac_dev_tools import from_environment_as_dataclass


@dataclasses.dataclass
class DefaultConfig:
    HOST : str = 'localhost'
    PORT : int = 8080
    DEBUG : bool = False
    OPENID_URL : str = ''
    OPENID_AUDIENCE : str = ''
    ICEPROD_CRED_ADDRESS : str = 'https://credentials.iceprod.icecube.aq'
    ICEPROD_CRED_CLIENT_ID : str = ''
    ICEPROD_CRED_CLIENT_SECRET : str = ''
    DB_URL: str = 'mongodb://localhost/iceprod'
    DB_TIMEOUT: int = 60
    DB_WRITE_CONCERN: int = 1
    PROMETHEUS_PORT: int = 0
    S3_ADDRESS: str = ''
    S3_ACCESS_KEY: str = ''
    S3_SECRET_KEY: str = ''
    MAX_BODY_SIZE: int = 10**9
    ROUTE_STATS_WINDOW_SIZE: int = 1000
    ROUTE_STATS_WINDOW_TIME: int = 3600
    ROUTE_STATS_TIMEOUT: int = 60
    CI_TESTING: str = ''


def get_config() -> DefaultConfig:
    return from_environment_as_dataclass(DefaultConfig)
