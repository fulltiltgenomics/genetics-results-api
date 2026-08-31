"""Startup warming is derived from the registry, not restated beside it.

The failure this pins is silent: a service registered in `app.core.service_container`
but absent from a second list in the lifespan still works, it is just cold, and the
only symptom is one slow request in production. `register()` therefore takes a
mandatory `Warm`, and the lifespan asks the container what to warm. These tests check
both halves — that every registration carries a declaration, and that the lifespan
does not name services of its own.
"""

import ast
import asyncio
import pathlib
import threading
import time

import pytest

from app.core.service_container import ServiceContainer, Warm, container

SERVER_SOURCE = pathlib.Path(__file__).resolve().parents[1] / "app" / "server.py"


def test_every_registered_service_declares_a_warm_policy():
    declared = set().union(*(set(container.warm_names(k)) for k in Warm))
    assert declared == set(container._factories)


def test_registering_without_a_declaration_fails():
    """The gate: a new service cannot reach the registry without stating an intent."""
    fresh = ServiceContainer()

    with pytest.raises(TypeError):
        fresh.register("new_service", lambda: object())

    with pytest.raises(TypeError):
        fresh.register("new_service", lambda: object(), "thread")

    # picking NONE is a decision, so it has to be written down
    with pytest.raises(ValueError, match="without a reason"):
        fresh.register("new_service", lambda: object(), Warm.NONE)

    fresh.register("new_service", lambda: object(), Warm.NONE, "nothing to prefetch")
    assert fresh.warm_names(Warm.NONE) == ["new_service"]


def test_the_lifespan_names_no_services_of_its_own():
    """No second list: `lifespan` may not mention a registered service by name."""
    tree = ast.parse(SERVER_SOURCE.read_text())
    lifespan = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "lifespan"
    )
    registered = set(container._factories)
    named = {
        node.value
        for node in ast.walk(lifespan)
        if isinstance(node, ast.Constant) and node.value in registered
    }
    assert named == set()


def test_dataset_mapping_is_warmed_off_the_event_loop():
    """`DatasetMapping.__init__` does a synchronous fsspec read of every mapping file.

    The lifespan smoke query forces that construction during startup either way (it runs
    `stream_range`, and every `tsv_line_iterator_*` binds the mapping). Declaring it
    THREAD is what decides WHICH thread pays: a worker rather than the serving loop.
    """
    assert container.warm_policy("dataset_mapping") is Warm.THREAD


def _blocking_factory(seconds: float, built: list):
    def factory():
        time.sleep(seconds)
        built.append(threading.current_thread().name)
        return object()

    return factory


async def _count_loop_ticks(work, interval=0.002) -> int:
    """Run `work` while ticking the loop, and report how many ticks got through."""
    ticks = 0
    task = asyncio.ensure_future(work)
    while not task.done():
        await asyncio.sleep(interval)
        ticks += 1
    await task
    return ticks


def test_thread_warming_leaves_the_event_loop_responsive():
    """A THREAD service's blocking construction must not stall the loop.

    Asserted against a control: the same factory resolved directly on the loop — what a
    first request does for anything left cold — wedges it for the whole read.
    """
    block = 0.3

    async def scenario():
        warmed_built: list = []
        warmed = ServiceContainer()
        warmed.register("slow", _blocking_factory(block, warmed_built), Warm.THREAD)
        warmed_ticks = await _count_loop_ticks(warmed.warm_registered())

        cold_built: list = []
        cold = ServiceContainer()
        cold.register("slow", _blocking_factory(block, cold_built), Warm.NONE, "control")

        async def first_request():
            # what a request pays for an unwarmed service: the factory, inline, on the loop
            cold.get("slow")

        cold_ticks = await _count_loop_ticks(first_request())

        return warmed_ticks, cold_ticks, warmed_built, cold_built

    warmed_ticks, cold_ticks, warmed_built, cold_built = asyncio.run(scenario())

    assert warmed_built and "MainThread" not in warmed_built[0]
    assert cold_built == ["MainThread"]
    assert warmed_ticks > 10, f"loop only ticked {warmed_ticks} times during warming"
    assert cold_ticks <= 1


def test_async_warming_awaits_warm_all():
    class Warmable:
        def __init__(self):
            self.warmed = False

        async def warm_all(self):
            self.warmed = True

    fresh = ServiceContainer()
    fresh.register("warmable", Warmable, Warm.ASYNC)
    fresh.register("cold", object, Warm.NONE, "control")

    asyncio.run(fresh.warm_registered())

    assert fresh.get("warmable").warmed is True
    assert fresh.is_initialized("cold") is False
