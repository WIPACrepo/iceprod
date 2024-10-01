"""
Test script for dataset_prio
"""

import logging
import uuid

from iceprod.server import dataset_prio

logger = logging.getLogger('dataset_prio_test')


def test_20_calc_dataset_prio():
    """Test calc_dataset_prio"""
    dataset = {'dataset_id': uuid.uuid1().hex,
               'dataset': 1,
               'tasks_submitted': 0,
               'priority': 0,
              }
    prio1 = dataset_prio.calc_dataset_prio(dataset)
    if not isinstance(prio1,(int,float)):
        raise Exception('dataset prio is not a number')

    dataset = {'dataset_id': uuid.uuid1().hex,
               'dataset': 1,
               'tasks_submitted': 0,
               'priority': 1,
              }
    prio2 = dataset_prio.calc_dataset_prio(dataset)
    if prio2 < prio1:
        raise Exception('priority is not winning')

    dataset = {'dataset_id': uuid.uuid1().hex,
               'dataset': 1,
               'tasks_submitted': 100,
               'priority': 1,
              }
    prio3 = dataset_prio.calc_dataset_prio(dataset)
    if prio2 < prio3:
        raise Exception('greater # tasks submitted is not losing')

    dataset = {'dataset_id': uuid.uuid1().hex,
               'dataset': 2,
               'tasks_submitted': 0,
               'priority': 1,
              }
    prio4 = dataset_prio.calc_dataset_prio(dataset)
    if prio2 < prio4:
        raise Exception('greater dataset_id is not losing')

    dataset = {'dataset_id': uuid.uuid1().hex,
               'dataset': 1,
               'tasks_submitted': 0,
               'priority': -1,
              }
    prio5 = dataset_prio.calc_dataset_prio(dataset)
    if prio5 != prio1:
        raise Exception('negative prio not reset to 0')


def test_21_calc_datasets_prios():
    """Test calc_datasets_prios"""
    dataset = {'dataset_id': uuid.uuid1().hex,
               'dataset': 1,
               'tasks_submitted': 0,
               'priority': 0,
              }
    dataset2 = {'dataset_id': uuid.uuid1().hex,
               'dataset': 2,
               'tasks_submitted': 0,
               'priority': 0,
              }
    datasets = {dataset['dataset_id']:dataset,
               dataset2['dataset_id']:dataset2}

    prios = dataset_prio.calc_datasets_prios(datasets)
    for p in prios.values():
        if not isinstance(p,(int,float)):
            raise Exception('dataset prio is not a number')
    if prios[dataset['dataset_id']] != prios[dataset2['dataset_id']]:
        logger.info(prios)
        raise Exception('datasets not equal in priority')

    dataset = {'dataset_id': uuid.uuid1().hex,
               'dataset': 1,
               'tasks_submitted': 0,
               'priority': 1,
              }
    dataset2 = {'dataset_id': uuid.uuid1().hex,
               'dataset': 2,
               'tasks_submitted': 0,
               'priority': 1,
              }
    datasets = {dataset['dataset_id']:dataset,
               dataset2['dataset_id']:dataset2}

    prios = dataset_prio.calc_datasets_prios(datasets)
    for p in prios.values():
        if not isinstance(p,(int,float)):
            raise Exception('dataset prio is not a number')
    if prios[dataset['dataset_id']] <= prios[dataset2['dataset_id']]:
        logger.info(prios)
        raise Exception('datasets in wrong order')

