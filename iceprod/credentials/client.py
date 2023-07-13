import argparse
import asyncio
from ..client_auth import add_auth_to_argparse, create_rest_client


async def main():
    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers(title='Cred Types', description='Register a credential', required=True)

    def generic_args(p):
        p.add_argument('url', default='https://data.icecube.aq', help='data base url')
        g = p.add_mutually_exclusive_group(required=True)
        g.add_argument('--user', help='username to assign cred to')
        g.add_argument('--group', help='group to assign cred to')

    parser_s3 = subparsers.add_parser('S3', aliases=['s3'], help='Register an S3 credential')
    parser_s3.set_defaults(type_='s3')
    generic_args(parser_s3)
    parser_s3.add_argument('access_key', metavar='access-key', help='s3 access key')
    parser_s3.add_argument('secret_key', metavar='secret-key', help='s3 secret key')
    parser_s3.add_argument('bucket', action='append', help='bucket(s) available (use multiple times for more buckets)')

    parser_oauth = subparsers.add_parser('OAuth', aliases=['oauth'], help='Register an OAuth credential', description='Must include either an access or refresh token')
    parser_oauth.set_defaults(type_='oauth')
    generic_args(parser_oauth)
    parser_oauth.add_argument('--access-token', dest='access_token', help='access token')
    parser_oauth.add_argument('--refresh-token', dest='refresh_token', help='refresh token')
    parser_oauth.add_argument('--expire-date', dest='expire_date', type=float, default=None, help='(optional) manual expiration date in unix time')
    parser_oauth.add_argument('--last-use', dest='last_use', type=float, default=None, help='(optional) manual last use date in unix time')

    add_auth_to_argparse(parser)
    args = parser.parse_args()

    if args.type_ == 'oauth' and not (args.access_token or args.refresh_token):
        raise argparse.ArgumentError(argument=args.refresh_token, message='--access-token or --refresh-token is required')

    args.rest_url = 'https://credentials.iceprod.icecube.aq'
    rc = create_rest_client(args)

    if args.user:
        url = f'/users/{args.user}/credentials'
    else:
        url = f'/groups/{args.group}/credentials'

    data = {
        'url': args.url,
        'type': args.type_,
    }

    if args.type_ == 's3':
        data.update({
            'access_key': args.access_key,
            'secret_key': args.secret_key,
            'buckets': args.bucket,
        })
    elif args.type_ == 'oauth':
        if args.access_token:
            data['access_token'] = args.access_token
        if args.refresh_token:
            data['refresh_token'] = args.refresh_token
        if args.expire_date:
            data['expire_date'] = args.expire_date
        if args.last_use:
            data['last_use'] = args.last_use
    else:
        raise RuntimeError('bad type')

    await rc.request('POST', url, data)


if __name__ == '__main__':
    asyncio.run(main())
