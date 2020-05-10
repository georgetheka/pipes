# Pipes

Pipes is a mini library for writing functional-ish pipelines in python.


## Overview

Pipes is a poc-level implementation of a MR-like functional pipeline that attempts to emulate pipelines
commonly found in functional/functional-ish technologies such as java, scala, f#, etc. It strives
to reduce the amount of imperative style code in python in order to improve correctness, tracing,
reduce mutability and conditional branching. As a side-effect, it may increase referential transparency and allows for concurrent computing
as demonstrated in this implementation using a simple thread pool (not yet optimized).

This framework has been literally written in one day as an exploratory experiment that seemed to turn out okay.
Although, there's an accompanying core pytest suite, it is not recommended for production use without further
testing and optimizations.


### TODO
- Optimize performance for non-deferred function types
- Allow more concurrency options including process pools, bring-your-own-pool, and async/await


## Usage

### Hello World

Given the following data and predicate functions:
```python
data = [
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
    ...
]


def has_blue_color(item):
    return item['color'] == 'blue'


def has_square_shape(item):
    return item['shape'] == 'square'
```
We can run the following pipeline to compute the doubled sizes for items where color=blue, and
shape=square. Note that deferred functions are prefixed with lz (as in lazy) which ensures that
a pipeline with M deferred pipes processing N items won't result in M iterations of N but only one iteration of N. 

```python
from pipes import Pipes


doubled_blue_square_sizes = (Pipes(data)
                    .lz_filter(has_blue_color)
                    .lz_filter(has_square_shape)
                    .lz_map(lambda x: x['size'])
                    .lz_map(lambda x: x * 2)
                    .apply()) 
```
The same example, but processed in a parallel using a thread pool:
```python
with Pipes(data, num_threads=8) as pipes:
    doubled_blue_square_sizes = (pipes
                        .lz_filter(has_blue_color)
                        .lz_filter(has_square_shape)
                        .lz_map(lambda x: x['size'])
                        .lz_map(lambda x: x * 2)
                        .apply()) 
```

Handling raised exceptions can be done by supplying additional optional parameters:

- `error_log_fn`: allows for capturing and logging the exception and values
- `error_val_fn`: provides a way to place or compute a default value in case of an exception 

```python
def square_and_raise_on_3(x):
    if x == 3:
        raise ValueError('unaccepted value')
    return x ** 2

result = (Pipes(data)
          .lz_map(lambda x: x['size'])
          .lz_peek(lambda x: LOG.debug(f'value={x}'))
          .lz_filter(
                square_and_raise_on_3,
                error_log_fn=lambda x, v, nv: 
                    LOG.error(f'Error: "{x}" for val = {v}, defaulting to {nv}'),
                error_val_fn=lambda x: x * 100,
            )
          .reduce(lambda a, b: a + b, init_val=0))
```
For more examples, see [examples.py](./examples.py) and [pipes/test_pipes.py](./pipes/test_pipes.py).

### API

The following operations are supported. See docstrings in [pipes/pipes.py](./pipes/pipes.py) for more information:

- `lz_map`: deferred map operator
- `lz_filter`: deferred filter based on a predicator function
- `lz_peek`: deferred peek allows peeking or collecting value after any pipe
- `count`: computes length of collection
- `uniq`: dedupes a collection based on the default or a supplied hashing function
- `sort`: sorts a collection based on a key function
- `flat_map`: flat-maps using a custom function 
- `apply`: terminal operator that executes entire pipeline
- `stream`: like apply but creates an generator instead
- `group_by`: partitions a collection based on a key function and group function
- `reduce`: performs a classic reduce operator

The `Pair` class:

It is often useful to propagate a tuple down a pipeline that can encapsulate both an initial
and a computed value (lvalue/rvalue). Therefore a simple helper class is supplied to help
accomplish this in cleaner way than a python tuple.

```python
from pipes import Pair


p = Pair(L='lvalue', R='rvalue')
p.L # lvalue
p.R # rvalue
```
