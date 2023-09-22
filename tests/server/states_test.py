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
    assert states.dataset_prev_statuses('complete') == ['processing']
    assert states.dataset_prev_statuses.cache_info().hits == 0
    states.dataset_prev_statuses('complete')
    assert states.dataset_prev_statuses.cache_info().hits == 1


def test_status_sort():
    st = {
        'a': ['b'],
        'b': ['c'],
        'c': [],
    }
    assert sorted(['c', 'a'], key=partial(states.status_sort, st)) == ['a', 'c']
