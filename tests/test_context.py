import asyncio
import concurrent.futures
import pytest
import roax.context as context
import time


def test_basic():
    assert sum(1 for _ in context.find()) == 0
    with context.push(context="foo", value=1):
        with context.push(context="foo", value=2):
            assert context.last(context="root") is not None
            assert sum(1 for _ in context.find()) == 3
            assert sum(1 for _ in context.find()) == 3
            assert context.first(context="foo")["value"] == 1
            assert context.last(context="foo")["value"] == 2
            assert sum(1 for _ in context.find(context="foo")) == 2
    assert sum(1 for _ in context.find()) == 0


def test_thread_isolation():
    def thread(seed):
        assert sum(1 for _ in context.find()) == 0  # threads do not inherit from invoker
        for n in range(0, 10):
            with context.push(context="thread1", seed=seed):
                time.sleep(0.01)
                assert context.last(context="thread1")["seed"] == seed
                seed2 = seed + 1
                with context.push(context="thread2", seed=seed2):
                    time.sleep(0.01)
                    assert context.last(context="thread2")["seed"] == seed2
        assert sum(1 for _ in context.find()) == 0

    assert sum(1 for _ in context.find()) == 0
    with context.push(context="main"):
        assert sum(1 for _ in context.find()) == 2  # main and root
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(thread, s) for s in [1000, 2000, 3000]]
            for future in concurrent.futures.as_completed(futures):
                future.result()
        assert sum(1 for _ in context.find()) == 2
    assert sum(1 for _ in context.find()) == 0


def test_coroutine_isolation():
    async def coro(seed):
        assert sum(1 for _ in context.find()) == 2  # coroutines inherit from caller
        for n in range(0, 10):
            with context.push(context="coro1", seed=seed):
                await asyncio.sleep(0.01)
                assert context.last(context="coro1")["seed"] == seed
                seed2 = seed + 1
                with context.push(context="coro2", seed=seed2):
                    await asyncio.sleep(0.01)
                    assert context.last(context="coro2")["seed"] == seed2
        assert sum(1 for _ in context.find()) == 2

    async def run():
        assert sum(1 for _ in context.find()) == 0
        with context.push(context="main"):
            assert sum(1 for _ in context.find()) == 2  # main and root
            coroutines = [coro(s) for s in [1000, 2000, 3000]]
            await asyncio.gather(*coroutines)
            assert sum(1 for _ in context.find()) == 2
        assert sum(1 for _ in context.find()) == 0

    asyncio.run(run())
