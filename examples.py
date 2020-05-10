import logging

from pipes import Pipes, Pair

LOG = logging.getLogger(__name__)

_EXAMPLE_COLLECTION = [
    {
        'color': 'blue',
        'shape': 'square',
        'size': 1,
        'items': ['a', 'b', 'c', ],
    },
    {
        'color': 'blue',
        'shape': 'square',
        'size': 2,
        'items': ['d', 'e', 'f', ],
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


def example1():
    def has_blue_color(item):
        return item['color'] == 'blue'

    def has_square_shape(item):
        return item['shape'] == 'square'

    num_blue_squares = (Pipes(_EXAMPLE_COLLECTION)
                        .lz_filter(has_blue_color)
                        .lz_filter(has_square_shape)
                        .count())

    print(f'Number of blue squares = {num_blue_squares}')


def example2():
    """Demonstrates, flatmap, sort, uniq, and parallel processing
    """

    def has_red_color(item):
        return item['color'] == 'red'

    with Pipes(_EXAMPLE_COLLECTION, num_threads=4) as p:
        sorted_unique_red_items = (p
                                   .lz_filter(has_red_color)
                                   .flat_map(lambda x: x['items'])
                                   .sort()
                                   .uniq()
                                   .apply())

    print(f'Number of items for red circles = {sorted_unique_red_items}')


def example3():
    """Demonstrates exception handling, peek, reduce.
    """

    def square_and_raise_on_3(x):
        if x == 3:
            raise ValueError('unaccepted value')
        return x ** 2

    result = (Pipes(_EXAMPLE_COLLECTION)
              .lz_map(lambda x: x['size'])
              .lz_peek(lambda x: LOG.debug(f'value={x}'))
              .lz_filter(
        square_and_raise_on_3,
        error_log_fn=lambda x, v, nv: LOG.error(f'Error: "{x}" for val = {v}, defaulting to {nv}'),
        error_val_fn=lambda x: x * 100,
    )
              .reduce(lambda a, b: a + b, init_val=0))

    print(f'map/reduce result = {result}')


def example4():
    """Demonstrates group_by, pair.
    """
    result = (Pipes(_EXAMPLE_COLLECTION)
              .lz_map(lambda x: Pair(L=x['shape'], R=x['color']))
              .lz_filter(lambda pair: pair.L == 'square')
              .group_by(lambda pair: pair.R, group_fn=lambda group: len(list(group))))

    print(f'group by result = {result}')


def example5():
    """Demonstrates streams.
    """

    def gen():
        for i in range(1000):
            yield i

    items = []
    with Pipes(gen(), num_threads=8) as pipes:
        generator = pipes.lz_map(lambda x: x ** 2).stream()
        while True:
            try:
                v = next(generator)
                items.append(v)
            except StopIteration:
                break

    print(f'total items = {len(items)}')


if __name__ == '__main__':
    example1()
    example2()
    example3()
    example4()
    example5()
