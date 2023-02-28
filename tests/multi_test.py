import multiprocessing
import os
import threading
import time
from typing import Tuple

import pytest

from searvey import multi


# Some help functions to test multithreading
def get_threadname(**kwargs) -> Tuple[str, str]:
    # We add a tiny amount of wait_time to make sure that all the threads are getting used.
    time.sleep(0.001)
    return threading.current_thread().name


def get_processname(**kwargs) -> Tuple[str, str]:
    # We add a tiny amount of wait_time to make sure that all the processes are getting used.
    time.sleep(0.05)
    return multiprocessing.current_process().name


def raise_zero_division_error(**kwargs) -> None:
    raise ZeroDivisionError()


def return_one(number) -> float:
    return 1


# The actual tests
def test_multiprocess_raises_value_error_if_n_workers_higher_than_available_threads() -> None:
    with pytest.raises(ValueError) as exc:
        multi.multiprocess(func=lambda x: x, func_kwargs=[dict(x=1)] * 2, n_workers=1024)
    assert f"The maximum available processes are {multi.MAX_AVAILABLE_PROCESSES}, not: 1024" == str(
        exc.value
    )


@pytest.mark.parametrize(
    "concurrency_func",
    [multi.multithread, multi.multiprocess],
)
def test_concurrency_functions_returns_FutureResult(concurrency_func) -> None:
    results = concurrency_func(
        func=return_one,
        func_kwargs=[dict(number=n) for n in (1, 2, 3)],
    )
    for result in results:
        assert isinstance(result, multi.FutureResult)
        assert result.result == 1
        assert result.exception is None


@pytest.mark.parametrize(
    "concurrency_func",
    [multi.multithread, multi.multiprocess],
)
def test_concurrency_functions_returns_FutureResult_even_when_exceptions_are_raised(
    concurrency_func,
) -> None:
    results = concurrency_func(
        func=raise_zero_division_error,
        func_kwargs=[dict(number=n) for n in (1, 2, 3)],
    )
    for result in results:
        assert isinstance(result, multi.FutureResult)
        assert result.result is None
        assert isinstance(result.exception, ZeroDivisionError)


@pytest.mark.parametrize("n_workers", [1, 2, 4])
def test_multithread_pool_size(n_workers) -> None:
    if n_workers == 4 and os.environ.get("CI", False):
        pytest.skip("Github actions only permits 2 concurrent threads")
    # Test that the number of the used threads is equal to the specified number of workers
    results = multi.multithread(
        func=get_threadname, func_kwargs=[{"arg": i} for i in range(4 * n_workers)], n_workers=n_workers
    )
    thread_names = {result.result for result in results}
    assert len(thread_names) == n_workers


@pytest.mark.parametrize("n_workers", [1, 2, 4])
def test_multiprocess_pool_size(n_workers) -> None:
    if n_workers == 4 and os.environ.get("CI", False):
        pytest.skip("Github actions only permits 2 concurrent processes")
    # Test that the number of the used processes is equal to the specified number of workers
    results = multi.multiprocess(
        func=get_processname, func_kwargs=[{"arg": i} for i in range(4 * n_workers)], n_workers=n_workers
    )
    process_names = {result.result for result in results}
    assert len(process_names) == n_workers
