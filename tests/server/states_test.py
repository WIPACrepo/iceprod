from functools import partial
from iceprod.server import states


def test_get_all_prev_statuses():
    st = {
        'a': ['b'],
        'b': ['c'],
        'c': [],
    }
    assert states.get_all_prev_statuses(st, 'b') == ['a']


def test_dataset_prev_status():
    # other tests may have warmed the cache
    states.dataset_prev_statuses.cache_clear()
    assert states.dataset_prev_statuses('complete') == ['processing']
    assert states.dataset_prev_statuses.cache_info().hits == 0
    states.dataset_prev_statuses('complete')
    assert states.dataset_prev_statuses.cache_info().hits == 1


def test_dataset_obsolete_transitions():
    # any status can become obsolete
    assert set(states.dataset_prev_statuses('obsolete')) == {'processing', 'suspended', 'errors', 'complete'}
    # obsolete can be restored (suspended) or reset (processing)
    assert states.DATASET_STATUS['obsolete'] == ['suspended', 'processing']
    assert 'obsolete' in states.dataset_prev_statuses('suspended')
    assert 'obsolete' in states.dataset_prev_statuses('processing')
    assert 'obsolete' not in states.dataset_prev_statuses('complete')
    assert 'obsolete' not in states.dataset_prev_statuses('errors')
    # obsolete sorts last
    assert states.dataset_status_sort('obsolete') == len(states.DATASET_STATUS) - 1


def test_status_sort():
    st = {
        'a': ['b'],
        'b': ['c'],
        'c': [],
    }
    assert sorted(['c', 'a'], key=partial(states.status_sort, st)) == ['a', 'c']
