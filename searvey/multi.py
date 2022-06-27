"""
Helpers for multi processing/threading
"""
from __future__ import annotations

import logging
import os
from collections.abc import Callable
from concurrent.futures import as_completed
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Type
from typing import Union

import pydantic
import tqdm

# FTR, `loky.ProcessPoolExecutor` is more robust WRT pickling big objects
# But since we are not using MultiProcessing here, we don't need to introduce
# an additional dependency
# from loky import ProcessPoolExecutor

logger = logging.getLogger(__name__)


# https://docs.python.org/3/library/os.html#os.cpu_count
try:
    MAX_AVAILABLE_PROCESSES = len(os.sched_getaffinity(0))
except AttributeError:
    MAX_AVAILABLE_PROCESSES = os.cpu_count()
    if MAX_AVAILABLE_PROCESSES is None:
        MAX_AVAILABLE_PROCESSES = 1


class FutureResult(pydantic.BaseModel):
    exception: Optional[Exception] = None
    kwargs: Optional[Dict[str, Any]] = None
    result: Any = None

    class Config:
        arbitrary_types_allowed: bool = True

    def __hash__(self) -> int:
        return hash((type(self),) + tuple(self.__dict__.values()))


def multi(
    executor: Union[Type[ProcessPoolExecutor], Type[ThreadPoolExecutor]],
    func: Callable[..., Any],
    func_kwargs: List[Dict[str, Any]],
    n_workers: int,
    print_exceptions: bool = True,
    include_kwargs: bool = True,
    initializer: Optional[Callable[..., Any]] = None,
    disable_progress_bar: bool = True,
) -> List[FutureResult]:
    with tqdm.tqdm(total=len(func_kwargs), disable=disable_progress_bar) as progress_bar:
        with executor(max_workers=n_workers, initializer=initializer) as xctr:
            futures_to_kwargs = {xctr.submit(func, **kwargs): kwargs for kwargs in func_kwargs}
            results = []
            for future in as_completed(futures_to_kwargs):
                result_kwargs: Optional[Dict[str, Any]] = futures_to_kwargs[future]
                try:
                    func_result = future.result()
                except Exception as exc:
                    if print_exceptions:
                        print(f"<{result_kwargs}> generated an exception: {exc}")
                    if not include_kwargs:
                        result_kwargs = None
                    results.append(FutureResult(exception=exc, kwargs=result_kwargs))
                else:
                    if not include_kwargs:
                        result_kwargs = None
                    results.append(FutureResult(result=func_result, kwargs=result_kwargs))
                finally:
                    progress_bar.update(1)
            return results


def multithread(
    func: Callable[..., Any],
    func_kwargs: List[Dict[str, Any]],
    n_workers: int = max(1, MAX_AVAILABLE_PROCESSES - 1),
    print_exceptions: bool = True,
    include_kwargs: bool = True,
    executor: Type[ThreadPoolExecutor] = ThreadPoolExecutor,
    initializer: Optional[Callable[..., Any]] = None,
    disable_progress_bar: bool = True,
) -> List[FutureResult]:
    results = multi(
        executor=executor,
        func=func,
        func_kwargs=func_kwargs,
        n_workers=n_workers,
        print_exceptions=print_exceptions,
        include_kwargs=include_kwargs,
        initializer=initializer,
        disable_progress_bar=disable_progress_bar,
    )
    return results


def multiprocess(
    func: Callable[..., Any],
    func_kwargs: List[Dict[str, Any]],
    n_workers: int = max(1, MAX_AVAILABLE_PROCESSES - 1),
    print_exceptions: bool = True,
    include_kwargs: bool = True,
    executor: Type[ProcessPoolExecutor] = ProcessPoolExecutor,
    initializer: Optional[Callable[..., Any]] = None,
    disable_progress_bar: bool = True,
) -> List[FutureResult]:
    if n_workers > MAX_AVAILABLE_PROCESSES:
        msg = f"The maximum available processes are {MAX_AVAILABLE_PROCESSES}, not: {n_workers}"
        raise ValueError(msg)
    results = multi(
        executor=executor,
        func=func,
        func_kwargs=func_kwargs,
        n_workers=n_workers,
        print_exceptions=print_exceptions,
        include_kwargs=include_kwargs,
        initializer=initializer,
        disable_progress_bar=disable_progress_bar,
    )
    return results
