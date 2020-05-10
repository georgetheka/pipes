import random
import time
from collections.abc import Iterator
from itertools import groupby, chain
from typing import Sized, Any

from pipes import Pipes


class _CountableIterator:
    def __init__(self, itr: Iterator):
        self._count = 0
        self._itr = itr

    def gen(self) -> Iterator:
        for i in self._itr:
            self._count += 1
            yield i

    @property
    def count(self) -> int:
        return self._count


_KNOWN_RANGE_START = 1
_KNOWN_RANGE_END = 11
_KNOWN_RANGE = range(_KNOWN_RANGE_START, _KNOWN_RANGE_END)

_KNOWN_COLLECTION = [
    {
        'color': 'blue',
        'shape': 'square',
        'size': 1,
        'items': ['a', 'b', 'c', ],
    },
    {
        'color': 'red',
        'shape': 'square',
        'size': 2,
        'items': ['d', 'e', 'f', ],
    },
    {
        'color': 'red',
        'shape': 'circle',
        'size': 2,
        'items': ['a', 'a', 'b', ],
    },
    {
        'color': 'green',
        'shape': 'circle',
        'size': 1,
        'items': ['f', ],
    },
    {
        'color': 'blue',
        'shape': 'triangle',
        'size': 3,
        'items': ['c', 'd', 'e', 'a', ],
    },
    {
        'color': 'green',
        'shape': 'triangle',
        'size': 3,
        'items': ['f', 'g', ],
    },
]


def _assert_linear_performance(iterator: _CountableIterator, sized: Sized):
    assert iterator.count == len(sized)


def test_map_apply():
    # arrange
    iterator = _CountableIterator(_KNOWN_RANGE)
    expected = [i * 2 for i in _KNOWN_RANGE]
    # act
    actual = (Pipes(iterator.gen())
              .lz_map(lambda x: x * 2)
              .apply())
    # assert
    assert actual == expected
    _assert_linear_performance(iterator, _KNOWN_RANGE)


def test_multi_map_apply():
    # arrange
    iterator = _CountableIterator(_KNOWN_RANGE)
    expected = [1.0 / ((i + 1) * 2) for i in _KNOWN_RANGE]
    # act
    actual = (Pipes(iterator.gen())
              .lz_map(lambda x: x + 1)
              .lz_map(lambda x: x * 2)
              .lz_map(lambda x: 1.0 / x)
              .apply())
    # assert
    assert actual == expected
    _assert_linear_performance(iterator, _KNOWN_RANGE)


def test_map_filter_map_apply():
    # arrange
    iterator = _CountableIterator(_KNOWN_RANGE)
    expected = [(i * 10) + 1 for i in _KNOWN_RANGE if (i * 10) % 2 == 0]
    # act
    actual = (Pipes(iterator.gen())
              .lz_map(lambda x: x * 10)
              .lz_filter(lambda x: x % 2 == 0)
              .lz_map(lambda x: x + 1)
              .apply())
    # assert
    assert actual == expected
    _assert_linear_performance(iterator, _KNOWN_RANGE)


def test_multi_filter_apply():
    # arrange
    iterator = _CountableIterator(_KNOWN_RANGE)
    expected = [i for i in _KNOWN_RANGE if (i % 3 == 0) and (i % 9 == 0)]
    # act
    actual = (Pipes(iterator.gen())
              .lz_filter(lambda x: x % 3 == 0)
              .lz_filter(lambda x: x % 9 == 0)
              .apply())
    # assert
    assert actual == expected
    _assert_linear_performance(iterator, _KNOWN_RANGE)


def test_map_reduce():
    # arrange
    iterator = _CountableIterator(_KNOWN_RANGE)
    expected = sum([i for i in _KNOWN_RANGE])
    # act
    actual = (Pipes(iterator.gen())
              .reduce(lambda a, b: a + b, init_val=0))
    # assert
    assert actual == expected
    _assert_linear_performance(iterator, _KNOWN_RANGE)


def test_map_group_by():
    # arrange
    iterator = _CountableIterator(_KNOWN_COLLECTION)
    expected = {
        key: len(list(group))
        for key, group in groupby([e for e in _KNOWN_COLLECTION], lambda x: x['color'])
    }
    # act
    actual = (Pipes(iterator.gen())
              .lz_map(lambda x: x)
              .group_by(lambda x: x['color'], group_fn=lambda group: len(list(group))))
    # assert
    assert actual == expected
    _assert_linear_performance(iterator, _KNOWN_COLLECTION)


def test_map_flat_map_count():
    # arrange
    iterator = _CountableIterator(_KNOWN_COLLECTION)
    expected = len([x for x in
                    [e.upper() for e in chain.from_iterable(
                        [e['items'] for e in _KNOWN_COLLECTION])
                     ] if x == 'A'])
    # act
    actual = (Pipes(iterator.gen())
              .lz_map(lambda x: x)
              .flat_map(lambda x: x['items'])
              .lz_map(lambda x: x.upper())
              .lz_filter(lambda x: x == 'A')
              .count())
    # assert
    assert actual == expected
    _assert_linear_performance(iterator, _KNOWN_COLLECTION)


def test_map_uniq_count():
    # arrange
    iterator = _CountableIterator(_KNOWN_COLLECTION)
    expected = len(list(dict.fromkeys([e['color'] for e in _KNOWN_COLLECTION])))
    # act
    actual = (Pipes(iterator.gen())
              .lz_map(lambda x: x['color'])
              .uniq()
              .count())
    # assert
    assert actual == expected
    _assert_linear_performance(iterator, _KNOWN_COLLECTION)


def test_uniq_with_custom_hash():
    # arrange
    iterator = _CountableIterator(_KNOWN_COLLECTION)
    expected = len(list(dict.fromkeys([e['color'] for e in _KNOWN_COLLECTION])))
    # act
    actual = (Pipes(iterator.gen())
              .uniq(hash_fn=lambda x: hash(x['color']))
              .count())
    # assert
    assert actual == expected
    _assert_linear_performance(iterator, _KNOWN_COLLECTION)


def test_sort_apply():
    # arrange
    iterator = _CountableIterator(_KNOWN_COLLECTION)
    expected = sorted([e['color'] for e in _KNOWN_COLLECTION])
    # act
    actual = (Pipes(iterator.gen())
              .lz_map(lambda x: x['color'])
              .sort()
              .apply())
    # assert
    assert actual == expected
    _assert_linear_performance(iterator, _KNOWN_COLLECTION)


def test_reverse_sort_with_custom_hash_fn():
    # arrange
    iterator = _CountableIterator(_KNOWN_COLLECTION)
    expected = sorted([e['color'] for e in _KNOWN_COLLECTION], reverse=True)
    # act
    actual = (Pipes(iterator.gen())
              .sort(fn=lambda x: x['color'], reverse=True)
              .lz_map(lambda x: x['color'])
              .apply())
    # assert
    assert actual == expected
    _assert_linear_performance(iterator, _KNOWN_COLLECTION)


def test_peek():
    # arrange
    iterator = _CountableIterator(_KNOWN_COLLECTION)
    expected = [e['color'] for e in _KNOWN_COLLECTION]
    # act
    actual = []
    (Pipes(iterator.gen())
     .lz_map(lambda x: x['color'])
     .lz_peek(lambda x: actual.append(x))
     .apply())
    # assert
    assert actual == expected
    _assert_linear_performance(iterator, _KNOWN_COLLECTION)


def test_stream():
    # arrange
    iterator = _CountableIterator(_KNOWN_COLLECTION)
    expected = [e['color'] for e in _KNOWN_COLLECTION]
    # act
    actual = []
    for e in Pipes(iterator.gen()).lz_map(lambda x: x['color']).stream():
        actual.append(e)
    # assert
    assert actual == expected
    _assert_linear_performance(iterator, _KNOWN_COLLECTION)


def test_map_with_exception_handling():
    # arrange
    error = 'expected error'
    value = 5
    new_value = value * 100
    iterator = _CountableIterator(_KNOWN_RANGE)
    expected = [new_value if i == value else i * 2 for i in _KNOWN_RANGE]

    def raise_exception(x: Any) -> Any:
        if x == value:
            raise ValueError(error)
        return x * 2

    captured_errors = {}

    def error_log_fn(exc: Exception, v: Any, nv: Any) -> None:
        captured_errors['error'] = f'{exc}'
        captured_errors['value'] = v
        captured_errors['new_value'] = nv

    # act
    actual = (Pipes(iterator.gen())
              .lz_map(fn=raise_exception,
                      error_val_fn=lambda v: v * 100,
                      error_log_fn=error_log_fn)
              .apply())
    # assert
    assert actual == expected
    assert captured_errors['error'] == error
    assert captured_errors['value'] == value
    assert captured_errors['new_value'] == new_value
    _assert_linear_performance(iterator, _KNOWN_RANGE)


def test_end_to_end_parallel_pipeline_using_a_threadpool():
    # arrange
    iterator = _CountableIterator(_KNOWN_COLLECTION)
    expected = [e['shape'] for e in _KNOWN_COLLECTION]

    def run_task(x: Any) -> Any:
        # sleep between 1/5 (200ms) to 1/10 (100ms)
        time.sleep(1.0 / random.randrange(5, 10))
        return x['shape']

    start_time = time.time()
    # act
    actual = (Pipes(iterator.gen())
              .lz_map(run_task)
              .apply())
    actual_exec_time = time.time() - start_time

    start_time = time.time()

    with Pipes(iterator.gen(), num_threads=8) as pipes:
        threaded_actual = (pipes
                           .lz_map(run_task)
                           .apply())

    threaded_actual_exec_time = time.time() - start_time

    # assert
    assert actual == expected
    assert threaded_actual == expected
    assert actual_exec_time > threaded_actual_exec_time
