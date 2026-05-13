"""
Get storage plots for IceProd Datasets from Ceph
"""

import argparse
import asyncio
from collections import Counter
from enum import StrEnum
import json
import logging
from pathlib import Path
from typing import Any

import requests
from rest_tools.client import SavedDeviceGrantAuth
from iceprod.client_auth import add_auth_to_argparse, create_rest_client


class Group(StrEnum):
    ARA = 'ARA'
    BSM = 'BSM'
    CALIBRATION = 'Calibration'
    COSMIC_RAYS = 'Cosmic Rays'
    DIFFUSE = 'Diffuse'
    GEN2 = 'Gen2'
    GENERAL = 'General Sim'
    ICEACT = 'IceAct'
    LOW_ENERGY_ASTRO = 'Low Energy'
    NEUTRINO_SOURCES = 'Nu Sources'
    OFFLINE_PROCESSING = 'Offline'
    ONLINE_PROCESSING = 'Online'
    OSCILLATIONS = 'Oscillations'
    REALTIME = 'Realtime'
    SUPER_NOVA = 'Supernova'
    SYSTEMATICS_RECONSTRUCTION = 'Sys / Reco'
    UPGRADE = 'Upgrade'
    UNKNOWN = 'Uncategorized'


DATASET_CACHE = {}

async def get_dataset(num: str, iceprod_client) -> dict[str, Any]:
    global DATASET_CACHE
    n = int(num)
    if not DATASET_CACHE:
        logging.warning('Loading dataset cache...')
        ret = await iceprod_client.request('GET', '/datasets', {'keys': 'dataset_id|dataset|description|group|username'})
        if not ret:
            raise Exception('cannot get dataset info')
        DATASET_CACHE = {d['dataset']: d for d in ret.values()}
    return DATASET_CACHE[n]


SIMREQ_CACHE = {}

def get_simreq(dataset_id) -> dict[str, Any]:
    global SIMREQ_CACHE
    if not SIMREQ_CACHE:
        logging.warning('Loading simreq cache...')
        r = requests.get('https://simprod.icecube.wisc.edu/api/datasets/?format=json')
        r.raise_for_status()
        for row in r.json():
            if not row['simreq']:
                row.pop('simreq')
            SIMREQ_CACHE[row['did']] = row
        logging.info('simreq: %r', SIMREQ_CACHE)
    return SIMREQ_CACHE[dataset_id]


async def describe_path(path: Path, iceprod_client, leaf: bool = False) -> Group | None:
    """Try to match the path with a group"""
    logging.info('examining path %s', path)

    dataset_num = None
    if path.name.isnumeric() and len(path.name) == 5 and path.name.startswith('2'):
        dataset_num = path.name
    if (not dataset_num) and '.' in path.name:
        for p in path.name.split('.'):
            if p.isnumeric() and int(p) > 20000 and int(p) < 30000:
                dataset_num = p
                break

    if dataset_num:
        # we might have a dataset id
        try:
            dataset = await get_dataset(dataset_num, iceprod_client=iceprod_client)
        except KeyError:
            logging.info('apparently not a dataset', exc_info=True)
        else:
            logging.info('found dataset %d', dataset['dataset'])
            try:
                req = get_simreq(dataset['dataset_id'])
            except KeyError:
                logging.info('not part of a simreq')
            else:
                logging.info('found simreq')
                match req.get('simreq', {}).get('working_group','').lower():
                    case 'diffuse':
                        return Group.DIFFUSE
                    case 'bsm':
                        return Group.BSM
                    case 'calibration':
                        return Group.CALIBRATION
                    case 'cosmic rays': 
                        return Group.COSMIC_RAYS
                    case 'gen2':
                        return Group.GEN2
                    case 'icetop':
                        return Group.COSMIC_RAYS
                    case 'neutrino sources':
                        return Group.NEUTRINO_SOURCES
                    case 'oscillations':
                        return Group.OSCILLATIONS
                    case 'super nova':
                        return Group.SUPER_NOVA
                    case 'systematics/reconstruction':
                        return Group.SYSTEMATICS_RECONSTRUCTION
                    case 'general':
                        return Group.GENERAL
                match req.get('group', '').lower():
                    case 'diffuse':
                        return Group.DIFFUSE
                    case 'gen2':
                        return Group.GEN2
                    case 'icetop':
                        return Group.COSMIC_RAYS
                    case 'estes':
                        return Group.NEUTRINO_SOURCES
                    case 'oscillations':
                        return Group.OSCILLATIONS
                    case 'general':
                        return Group.GENERAL

            # try based on submitter
            match dataset['username']:
                case 'aleszczynska' | 'kath':
                    return Group.COSMIC_RAYS
                case 'rsnihur':
                    return Group.OFFLINE_PROCESSING

            # try based on dataset description
            desc = dataset['description'].lower()
            if 'icetop' in desc or desc.startswith('it '):
                return Group.COSMIC_RAYS
            elif 'genie' in desc:
                return Group.LOW_ENERGY_ASTRO
            elif 'offline filter data production' in desc:
                return Group.OFFLINE_PROCESSING
            elif 'diffuse' in desc or 'cascade' in desc:
                return Group.DIFFUSE
            elif 'neutrino-sources' in desc:
                return Group.NEUTRINO_SOURCES
            elif 'oscillations' in desc:
                return Group.OSCILLATIONS
            elif 'monopole' in desc:
                return Group.BSM
            #elif 'nugen' in desc or 'corsika-in-ice' in desc:
            #    return Group.GENERAL

    # basic path heuristics
    match path.name.lower():
        case 'ara':
            return Group.ARA
        case 'icetop' | 'icetopverify':
            return Group.COSMIC_RAYS
        case 'iceact':
            return Group.ICEACT
        case 'gen2':
            return Group.GEN2
        case 'genie' | 'deepcore' | 'lowenergyastro':
            return Group.LOW_ENERGY_ASTRO
        case 'oscillations':
            return Group.OSCILLATIONS
        case 'monopoles':
            return Group.BSM
        case 'sn' | 'supernova':
            return Group.SUPER_NOVA
        case 'lepton-injector' | 'bsm':
            return Group.BSM
        case 'cosmicray':
            return Group.COSMIC_RAYS
        case 'cscd':
            return Group.DIFFUSE
        case 'upgrade' | 'icecubeupgrade':
            return Group.UPGRADE
    if path.is_relative_to(Path('/data/exp')):
        match path.name.lower():
            case 'amanda' | 'aura' | 'dm-ice' | 'rice' | 'spase' | 'space-2' | 'spicecore':
                return Group.UNKNOWN
    if path.is_relative_to(Path('/data/exp/IceCube')) or path.is_relative_to(Path('/data/exp/IC40')):
        match path.name.lower():
            case 'testbed-sps-ara':
                return Group.ARA
            case 'calibration' | 'domcal':
                return Group.CALIBRATION
            case 'level2' | 'level2a' | 'level2pass2' | 'level2pass2a' | 'level2pass2-testdata' | 'pass3' | 'moon_ic59':
                return Group.OFFLINE_PROCESSING
            case 'level1' | 'unbiased' | 'sps-gcd' | 'pffilt' | 'dst' | 'dst_ic59' | 'dst_ic79' | 'pfnanodst':
                return Group.ONLINE_PROCESSING
            case 'ehwd' | 'scintillator' | 'testdaq' | 'hit-spooling' | 'hit-spooling-satellite' | 'hit-spooling-archive' | 'debugdata' | 'i3live' | 'i3ms' | 'pdaq-2ndbld' | 'southpole' | 'monitoring' | 'fat' | 'spat' | 'syssec' | 'sysmrtg' | 'i3db-backup/' | 'sysganglia' | 'nagios-rrd' | 'dom-testing':
                return Group.ONLINE_PROCESSING
            case 'sndaq': 
                return Group.SUPER_NOVA
            case 'level3-mu':
                return Group.NEUTRINO_SOURCES
            case 'level3-cscd' | 'moonsun_ic79':
                return Group.DIFFUSE
            case 'level3-earthwimp':
                return Group.BSM
    if path.is_relative_to(Path('/data/ana')):
        match path.name.lower():
            case 'diffuse' | 'cscd' | 'ehe' | 'earthcore':
                return Group.DIFFUSE
            case 'le':
                return Group.LOW_ENERGY_ASTRO
            case 'calibration' | 'iceproperties' | 'domeff':
                return Group.CALIBRATION
            case 'pointsource' | 'muon' | 'mese' | 'estes' | 'estes_ps' | 'grb' | 'starting-event' | 'level3-mu' | 'mufilter_study' | 'nusources' | 'hese_hs':
                return Group.NEUTRINO_SOURCES
            case 'sterileneutrino' | 'nufsgenmc' | 'gc_wimps' | 'level2w-wimp' | 'level3-wimp' | 'level4-wimp' | 'level5-wimp' | 'level6-wimp' | 'level7-wimp':
                return Group.BSM
            case 'oscillation':
                return Group.OSCILLATIONS
            case 'reconstruction' | 'in-ice-systematics':
                return Group.SYSTEMATICS_RECONSTRUCTION
            case 'fermi' | 'amanda':
                return Group.UNKNOWN
            case 'realtime' | 'followup':
                return Group.REALTIME
            case 'hex':
                return Group.GEN2
            case 'dst_ic79' | 'level2pass2b':
                return Group.OFFLINE_PROCESSING

    if dataset_num or leaf:
        # stop searching deeper, run path heuristics
        path_str = str(path).lower()
        if 'icetop' in path_str:
            return Group.COSMIC_RAYS
        elif 'iceact' in path_str:
            return Group.ICEACT
        elif 'gen2' in path_str:
            return Group.GEN2
        elif 'genie' in path_str or 'deepcore' in path_str:
            return Group.LOW_ENERGY_ASTRO
        elif 'muongun' in path_str:
            return Group.COSMIC_RAYS
        elif 'frb' in path_str or 'tracks' in path_str:
            return Group.NEUTRINO_SOURCES
        #elif 'neutrino-generator' in path_str or '_numu' in path_str or '_nue' in path_str or '_nutau' in path_str:
        #    return Group.NEUTRINO_SOURCES
        #elif 'corsika-in-ice' in path_str:
        #    return Group.COSMIC_RAYS
        elif 'data/exp' in path_str and ('level2' in path_str or 'pass2' in path_str):
                return Group.OFFLINE_PROCESSING
        elif 'data/exp' in path_str and ('level1' in path_str or 'pffilt' in path_str or 'daq' in path_str):
                return Group.ONLINE_PROCESSING
        return Group.UNKNOWN

    return None


ESTOP = False


async def get_size(path: Path, iceprod_client, ceph_client) -> dict[Group, int]:
    global ESTOP
    # children need to be examined and summed
    ret = Counter()
    try:
        data = await ceph_client.request('GET', str(path))
        for child in data['children']:
            if ESTOP or child['is_link']:
                continue

            child_path = Path(child['path'])
            leaf = (not child['is_dir']) or child['size'] < 1000000000 or len(child_path.parents) > (7 if path.is_relative_to('/data/sim') else 5)

            g = await describe_path(child_path, iceprod_client=iceprod_client, leaf=leaf)
            logging.info('%s = %s', child_path, g)
            if g:
                ret[g] += child['size']
            else:
                ret.update(await get_size(child_path, iceprod_client=iceprod_client, ceph_client=ceph_client))
    except asyncio.exceptions.CancelledError:
        logging.warning('CancelledError')
        ESTOP = True

    return ret


async def run(output, client):
    ceph = SavedDeviceGrantAuth(
        address='https://disk-usage.icecube.aq/api',
        token_url='https://keycloak.icecube.wisc.edu/auth/realms/IceCube',
        client_id='iceprod-public',
        filename='.disk_usage_token',
    )

    group_totals = Counter()
    sim_groups = await get_size(Path('/data/sim'), iceprod_client=client, ceph_client=ceph)
    group_totals.update(sim_groups)
    exp_groups = await get_size(Path('/data/exp'), iceprod_client=client, ceph_client=ceph)
    group_totals.update(exp_groups)
    ana_groups = await get_size(Path('/data/ana'), iceprod_client=client, ceph_client=ceph)
    group_totals.update(ana_groups)

    if output == '-':
        WIDTH = 13
        total = sum(group_totals.values())
        print(f"{'GROUP':{WIDTH}} {'DISK SPACE':>13} RELATIVE SIZE")
        for grp in Group:
            if grp not in group_totals:
                continue
            val = group_totals.get(grp, 0)
            print(f"{grp:{WIDTH}} {val/1000**4:>10.1f}TiB {'#' * (val*50//total)}")
        print(f"{'-'*WIDTH} {'-'*13}")
        print(f"{'Total':{WIDTH}} {total/1000**4:>10.1f}TiB")
    else:
        with open(output+'_sim', 'w') as f:
            json.dump(dict(sim_groups), f)
        with open(output+'_exp', 'w') as f:
            json.dump(dict(exp_groups), f)
        with open(output+'_ana', 'w') as f:
            json.dump(dict(ana_groups), f)
        with open(output, 'w') as f:
            json.dump(dict(group_totals), f)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-o', '--output', default='-', help='output config json to file (or "-" for stdout)')
    parser.add_argument('--log-level', default='INFO')
    add_auth_to_argparse(parser)
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level.upper()))

    client = create_rest_client(args, retries=0, timeout=5)

    asyncio.run(run(
        output=args.output,
        client=client,
    ))

if __name__ == '__main__':
    main()