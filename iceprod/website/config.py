import dataclasses

from wipac_dev_tools import from_environment_as_dataclass


@dataclasses.dataclass
class DefaultConfig:
    HOST : str = 'localhost'
    PORT : int = 8080
    DEBUG : bool = False
    OPENID_URL : str = ''
    OPENID_AUDIENCE : str = ''
    ICEPROD_WEB_URL : str = 'https://iceprod.icecube.aq'
    ICEPROD_API_ADDRESS : str = 'https://api.iceprod.icecube.aq'
    ICEPROD_API_CLIENT_ID : str = ''
    ICEPROD_API_CLIENT_SECRET : str = ''
    ICEPROD_CRED_ADDRESS : str = 'https://credentials.iceprod.icecube.aq'
    ICEPROD_CRED_CLIENT_ID : str = ''
    ICEPROD_CRED_CLIENT_SECRET : str = ''
    REDIS_HOST : str = 'localhost'
    REDIS_USER : str = ''
    REDIS_PASSWORD : str = ''
    REDIS_TLS : bool = False
    COOKIE_SECRET : str = ''
    PROMETHEUS_PORT: int = 0
    CI_TESTING: str = ''


def get_config() -> DefaultConfig:
    return from_environment_as_dataclass(DefaultConfig)
