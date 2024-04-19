"""
Functions used to calculate dataset priority.
"""

import math
import logging
from functools import partial
from collections import defaultdict


def apply_group_prios(datasets, groups=None, filters=None):
    """
    Apply the group priorities to the datasets.

    :param datasets: dataset dict with 'priority' and 'groups_id'
    :param groups: dump of groups table
    :param filters: any filters for the groups
    """
    if not groups:
        return datasets

    # first, calculate group priorities with filtering
    base_groups = {groups[g]['name']:groups[g]['priority'] for g in groups}
    if filters:
        filtered_groups = [g for g in base_groups if any(True for f in filters if f in g)]
    else:
        filtered_groups = base_groups.keys()

    group_prios = {}
    for g in filtered_groups:
        parts = g.split('/')
        prio = 1.0
        for i in range(2,len(parts)+1):
            print(i, '/'.join(parts[:i]))
            prio *= base_groups['/'.join(parts[:i])]
        subtract = 0.0
        for k in base_groups:
            if k.startswith(g) and len(k.split('/')) == len(parts)+1:
                subtract += base_groups[k]
        group_prios[g] = prio * (1.0 - subtract)

    # normalize dataset-group priorities
    norm = defaultdict(0.)
    for d in datasets:
        norm[datasets[d]['groups_id']] += datasets[g]['priority']

    # apply group priorities to datasets
    ret = {}
    for d in datasets:
        gid = datasets[d]['groups_id']
        if gid in groups:
            p_g = group_prios[groups[gid]['name']]
            if p_g > 0:
                ret[d] = datasets[d].copy()
                p_d = ret[d]['priority']
                if p_d <= 0:
                    ret[d]['priority'] = p_g
                else:
                    ret[d]['priority'] = p_d*1.0/norm[gid] * p_g
    return ret


def calc_dataset_prio(dataset, queueing_factor_priority=1.0,
                      queueing_factor_dataset=1.0, queueing_factor_tasks=1.0):
    """
    Calculate the dataset priority.

    :param dataset: a dataset with 'dataset_id', 'priority' and 'tasks_submitted'
    :param queueing_factor_priority: queueing factor for priority
    :param queueing_factor_dataset: queueing factor for dataset id
    :param queueing_factor_tasks: queueing factor for number of tasks
    """
    import math
    # priority factors
    qf_p = queueing_factor_priority
    qf_d = queueing_factor_dataset
    qf_t = queueing_factor_tasks

    # get dataset info
    p = dataset['priority']
    if p < 0 or p > 100:
        # do not allow negative or overly large priorities (they skew things)
        p = 0
        logging.warning('Priority for dataset %s is invalid, using default',dataset['dataset_id'])
    d = dataset['dataset']
    if d < 0:
        d = 0
        logging.warning('Dataset num for dataset %s is invalid, using default',dataset['dataset_id'])
    t = dataset['tasks_submitted']

    # return prio
    if t < 1:
        prio = (qf_p/10.0*p-qf_d/10000.0*d)
    else:
        prio = (qf_p/10.0*p-qf_d/10000.0*d-qf_t/10.0*math.log10(t))
    if prio < 0:
        prio = 0
        logging.error('Dataset prio for dataset %s is <0',dataset['dataset_id'])
    return prio


def calc_datasets_prios(datasets, queueing_factor_priority=1.0,
                        queueing_factor_dataset=1.0, queueing_factor_tasks=1.0):
    """
    Calculate the dataset priority for each dataset, normalized.

    :param datasets: dataset dict with 'dataset_id', 'priority' and 'tasks_submitted'
    :param queueing_factor_priority: queueing factor for priority
    :param queueing_factor_dataset: queueing factor for dataset id
    :param queueing_factor_tasks: queueing factor for number of tasks
    """
    calc = partial(calc_dataset_prio,
                   queueing_factor_priority=queueing_factor_priority,
                   queueing_factor_dataset=queueing_factor_dataset,
                   queueing_factor_tasks=queueing_factor_tasks)

    dataset_prios = {}
    for id in datasets:
        dataset_prios[id] = calc(datasets[id])
    logging.debug('dataset prios: %r',dataset_prios)
    # normalize
    total_prio = math.fsum(dataset_prios.values())
    if total_prio <= 0:
        # datasets do not have priority, so assign all equally
        for d in dataset_prios:
            dataset_prios[d] = 1.0/len(dataset_prios)
    else:
        for d in dataset_prios:
            dataset_prios[d] /= total_prio

    return dataset_prios
