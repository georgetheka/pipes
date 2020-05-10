from typing import NamedTuple, Any, Optional, Iterator

from pipes.pipes import _PipesBase


class Pair(NamedTuple):
    """Helper class for passing two parameters (left, right) through a pipeline.
    """
    L: Optional[Any]
    R: Optional[Any]


class Pipes(_PipesBase):
    """The pipeline class.
    """

    def __init__(self,
                 itr: Iterator[Any],
                 num_threads: int = 1,
                 ) -> None:
        super().__init__(
            itr=itr,
            num_threads=num_threads,
        )
