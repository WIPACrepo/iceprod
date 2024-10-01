import logging

from rest_tools.client import ClientCredentialsAuth, SavedDeviceGrantAuth
from wipac_dev_tools import from_environment


def add_auth_to_argparse(parser):
    """Add auth args to argparse."""
    config = from_environment({
        'API_URL': 'https://iceprod2-api.icecube.wisc.edu',
        'OAUTH_URL': 'https://keycloak.icecube.wisc.edu/auth/realms/IceCube',
        'OAUTH_CLIENT_ID': 'iceprod',
        'OAUTH_CLIENT_SECRET': '',
    })
    description = '''
        Use either user credentials or client credentials to authenticate.
        Can also be specified via env variables: API_URL, OAUTH_URL,
        OAUTH_CLIENT_ID, and OAUTH_CLIENT_SECRET.
    '''
    parser.add_argument('--rest-url', default=config['API_URL'],
                        help='URL for REST API (default: IceProd API)')
    group = parser.add_argument_group('OAuth', description)
    group.add_argument('--oauth-url', default=config['OAUTH_URL'],
                       help='The OAuth server URL for OpenID discovery')
    group.add_argument('--oauth-client-id', default=config['OAUTH_CLIENT_ID'],
                       help='The OAuth client id')
    group.add_argument('--oauth-client-secret', default=config['OAUTH_CLIENT_SECRET'],
                       help='The OAuth client secret, to enable client credential mode')


def create_rest_client(args):
    """Create a RestClient from argparse args."""
    if args.oauth_client_secret:
        logging.debug('Using client credentials to authenticate')
        return ClientCredentialsAuth(
            address=args.rest_url,
            token_url=args.oauth_url,
            client_id=args.oauth_client_id,
            client_secret=args.oauth_client_secret,
        )
    else:
        logging.debug('Using user credentials to authenticate')
        if args.oauth_client_id == 'iceprod':
            args.oauth_client_id = 'iceprod-public'
        return SavedDeviceGrantAuth(
            address=args.rest_url,
            filename='.iceprod-auth',
            token_url=args.oauth_url,
            client_id=args.oauth_client_id,
        )
