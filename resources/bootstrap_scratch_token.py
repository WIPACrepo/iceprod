#!/usr/bin/env python
import argparse
import logging
import asyncio

from rest_tools.client import DeviceGrantAuth, ClientCredentialsAuth


async def main():
    parser = argparse.ArgumentParser(description='job completion')

    parser.add_argument('--client-id-token', default='iceprod')
    parser.add_argument('client_secret_token')
    parser.add_argument('--client-id', default='iceprod')
    parser.add_argument('client_secret')
    parser.add_argument('--iceprod-url', default='https://credentials.iceprod.icecube.aq')
    parser.add_argument('--scratch-path', default='simprod')
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG)

    token_issuer = 'https://token-issuer.icecube.aq'

    token_kwargs = {
        'token_url': token_issuer,
        'client_id': args.client_id_token,
        'client_secret': args.client_secret_token,
        'scopes': [f'storage.read:/scratch/{args.scratch_path}', f'storage.modify:/scratch/{args.scratch_path}'],
    }
    rpc = DeviceGrantAuth('', **token_kwargs)
    rpc.refresh_token

    rpc_kwargs = {
        'token_url': 'https://keycloak.icecube.wisc.edu/auth/realms/IceCube',
        'client_id': args.client_id,
        'client_secret': args.client_secret,
    }
    rpc_cred = ClientCredentialsAuth(args.iceprod_url, **rpc_kwargs)

    try:
        await rpc_cred.request('POST', '/users/ice3simusr/credentials', {
            'url': token_issuer,
            'type': 'oauth',
            'transfer_prefix': f'osdf:///icecube/wipac/scratch/{args.scratch_path}',
            'refresh_token': rpc.refresh_token,
            'scope': ' '.join(token_kwargs['scopes'])
        })
    except Exception:
        await rpc_cred.request('PATCH', '/users/ice3simusr/credentials', {
            'url': token_issuer,
            'type': 'oauth',
            'transfer_prefix': f'osdf:///icecube/wipac/scratch/{args.scratch_path}',
            'refresh_token': rpc.refresh_token,
            'scope': ' '.join(token_kwargs['scopes'])
        })



if __name__ == '__main__':
    asyncio.run(main())
