import concurrent.futures
import enum
from concurrent.futures.thread import ThreadPoolExecutor
from functools import reduce
from itertools import groupby, chain
from typing import NamedTuple, Any, Optional, Callable, Iterator, List, Tuple, Iterable, Dict, Generator


class _Type(enum.Enum):
    """Enumerates the currently available lazy operations.
    """
    MAP = 10
    PEEK = 30
    FILTER = 40


class _Step(NamedTuple):
    """Encapsulates a single pipe step.
    """
    fn: Callable
    error_val_fn: Optional[Callable]
    error_log_fn: Optional[Callable]


class _Pipe(NamedTuple):
    """Encapsulates a single pipe. A pipe can have multiple steps.
    """
    type: _Type
    steps: List[_Step]


def _fn(s: _Step, v: Any) -> Optional[Any]:
    """Executes each step's function.
    :param s: the step to execute.
    :param v: the value to execute against.
    :return: computed value.
    """
    if s.error_log_fn:
        try:
            return s.fn(v)
        except Exception as exc:
            nv = s.error_val_fn(v) if s.error_val_fn else None
            s.error_log_fn(exc, v, nv)
            return nv
    return s.fn(v)


class _PipesBase:
    """the implementation for the pipes class.
    """

    def __init__(self,
                 itr: Iterator[Any],
                 num_threads: int = 1,
                 ) -> None:
        """Class Ctor.
        :param itr: iterator or generator to process.
        :param num_threads: the number of threads to use, defaults to 1.
        """
        self._itr = itr
        self._pipes = []
        self._num_threads = num_threads
        self._executor: Optional[ThreadPoolExecutor] = None

    def __enter__(self):
        """Implements with object() resource allocation interface.
        :return: self
        """
        self._executor = concurrent.futures.ThreadPoolExecutor(self._num_threads) \
            if self._num_threads > 1 else None
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """Implements with object() resource deconstruction interface.
        :param exc_type: Exception type.
        :param exc_value: Exception value.
        :param exc_traceback: Exception Traceback.
        """
        if self._executor:
            self._executor.shutdown()

    def _append(self, p_type: _Type,
                fn: Callable[[Any], Any],
                error_val_fn: Optional[Callable[[Any], Any]] = None,
                error_log_fn: Optional[Callable[[Exception, Any, Any], Any]] = None,
                ) -> None:
        """Appends each pipe to the pipeline.
        :param p_type: the pipe type.
        :param fn: the function to execute for this pipe.
        :param error_val_fn: the defaulting value function in case of a raised exception during fn execution.
        :param error_log_fn: the logging function in case of a raised exception during fn execution.
        """
        s = _Step(
            fn=fn,
            error_val_fn=error_val_fn,
            error_log_fn=error_log_fn,
        )

        if self._pipes and self._pipes[-1].type == p_type:
            self._pipes[-1].steps.append(s)
        else:
            self._pipes.append(_Pipe(type=p_type, steps=[s]))

    def _call(self, x) -> Tuple[bool, Any]:
        """Invokes each lazy step function.
        :param x: the value to execute against.
        :return: a flag indicating all predicates tests passed, computed value.
        """
        elem = x
        pred_tests_passed = True
        for p in self._pipes:
            if p.type == _Type.MAP:
                for s in p.steps:
                    elem = _fn(s, elem)
            elif p.type == _Type.PEEK:
                for s in p.steps:
                    _fn(s, elem)
            elif p.type == _Type.FILTER:
                for s in p.steps:
                    if not _fn(s, elem):
                        pred_tests_passed = False
                        break
                if not pred_tests_passed:
                    break
            else:
                pass
        return pred_tests_passed, elem

    def lz_map(self,
               fn: Callable[[Any], Any],
               error_val_fn: Optional[Callable[[Any], Any]] = None,
               error_log_fn: Optional[Callable[[Exception, Any, Any], Any]] = None,
               ) -> '_PipesBase':
        """Adds a lazy map step.
        :param fn: the function to execute.
        :param error_val_fn: the defaulting value function in case of a raised exception during fn execution.
        :param error_log_fn: the logging function in case of a raised exception during fn execution.
        :return: self
        """
        self._append(_Type.MAP,
                     fn=fn,
                     error_val_fn=error_val_fn,
                     error_log_fn=error_log_fn,
                     )
        return self

    def lz_filter(self,
                  fn: Callable[[Any], Any],
                  error_val_fn: Optional[Callable[[Any], Any]] = None,
                  error_log_fn: Optional[Callable[[Exception, Any, Any], Any]] = None,
                  ) -> '_PipesBase':
        """Adds a lazy filter step.
        :param fn: the function to execute.
        :param error_val_fn: the defaulting value function in case of a raised exception during fn execution.
        :param error_log_fn: the logging function in case of a raised exception during fn execution.
        :return: self
        """
        self._append(_Type.FILTER,
                     fn=fn,
                     error_val_fn=error_val_fn,
                     error_log_fn=error_log_fn,
                     )
        return self

    def lz_peek(self,
                fn: Callable[[Any], Any],
                error_val_fn: Optional[Callable[[Any], Any]] = None,
                error_log_fn: Optional[Callable[[Exception, Any, Any], Any]] = None,
                ) -> '_PipesBase':
        """Adds a lazy peek step. It can be used to either log or collect data from this pipe.
        :param fn: the function to execute.
        :param error_val_fn: the defaulting value function in case of a raised exception during fn execution.
        :param error_log_fn: the logging function in case of a raised exception during fn execution.
        :return: self
        """
        self._append(_Type.PEEK,
                     fn=fn,
                     error_val_fn=error_val_fn,
                     error_log_fn=error_log_fn,
                     )
        return self

    def apply(self) -> Iterator[Any]:
        """Executes the pipeline.
        :return: an iterator.
        """
        if self._executor:
            results = self._executor.map(self._call, self._itr)
        else:
            results = map(self._call, self._itr)
        return [value for is_collectible, value in results if is_collectible]

    def stream(self) -> Iterator[Any]:
        """Yields the results of the pipeline.
        :return: a generator instance.
        """
        for e in self.apply():
            yield e

    def flat_map(self, fn: Callable[[Any], Any]) -> '_PipesBase':
        """Flat map operation that is not deferred.
        :param fn: the function with which to flat-map.
        :return: self
        """
        self._itr = chain.from_iterable(map(fn, self.apply()))
        self._pipes = []
        return self

    def uniq(self, hash_fn: Optional[Callable[[Any], Any]] = None) -> '_PipesBase':
        """Creates a unique iterator based on the hash function, or __hash__ if none.
        Note that mutable values must supply their own hash_fn.
        :param hash_fn: the customer hash function.
        :return: self
        """
        u = {}
        for e in self.apply():
            h = hash_fn(e) if hash_fn else hash(e)
            if h not in u:
                u[h] = e
        self._itr = u.values()
        self._pipes = []
        return self

    def sort(self,
             fn: Optional[Callable[[Any], Any]] = None,
             reverse: bool = False,
             ) -> '_PipesBase':
        """Sorts the computed collection.
        :param fn: key function to use for sorting.
        :param reverse: flag to reverse sort order.
        :return: self
        """
        self._itr = sorted(self.apply(), key=fn, reverse=reverse)
        self._pipes = []
        return self

    def reduce(self,
               fn: Callable[[Any, Any], Any],
               init_val: Any,
               ) -> Any:
        """A terminal operator that reduces the collection baed on custom function and initial value.
        :param fn: function with which to reduce.
        :param init_val: accumulator seed value.
        :return: reduction result.
        """
        return reduce(fn, self.apply(), init_val)

    def count(self) -> int:
        """Counts item in collection.
        :return: collection length.
        """
        return len(list(self.apply()))

    def group_by(self,
                 fn: Optional[Callable[[Any], Any]] = None,
                 group_fn: Optional[Callable[[Iterable], Any]] = None,
                 ) -> Dict:
        """Groups by the given fn and group_fn functions.
        :param fn: the function with which to group by.
        :param group_fn: a function to further apply to each group.
        :return: a dict representation with keys as group-by partitions, and groups as values.
        """
        return {key: (group_fn(group) if group_fn else list(group))
                for key, group in groupby(self.apply(), fn if fn else None)}
