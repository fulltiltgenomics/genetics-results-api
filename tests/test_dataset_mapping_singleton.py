"""The dataset->resource mapping is built once per process.

`DatasetMapping.__init__` re-reads every entry of `dataset_mapping_files`, and
`read_file` handles `gs://`, so each extra construction is a network read of data
that never changes. Three sites used to construct their own; they now all resolve
the single container-held instance. This pins that: a regression here is silent
(everything still works, just slower), which is exactly the kind that survives.
"""

import ast
import inspect
import pathlib

import pytest

import app.core.streams as streams
import app.services.data_access_coloc as data_access_coloc
import app.services.dataset_mapping as dataset_mapping_module
from app.core.service_container import container

DATASET_MAPPING_MODULE = "app.services.dataset_mapping"


@pytest.fixture
def counting_mapping_file(monkeypatch):
    """Replace the configured mapping files with one in-memory file, counting reads.

    The container caches instances for the whole session, so the fake has to be
    evicted on both sides of the test or it leaks into every later test that touches
    the mapping — the order-dependent pass this test exists to prevent.
    """
    calls = []

    def fake_read_file(path):
        calls.append(path)
        return "dataset\tother\nDS_FAKE\tx\n"

    monkeypatch.setattr(dataset_mapping_module, "read_file", fake_read_file)
    monkeypatch.setattr(
        dataset_mapping_module,
        "dataset_mapping_files",
        [("gs://fake/mapping.tsv", "dataset", "fake_resource", "v1")],
    )
    # __init__ aliases and mutates the module-level dict, so hand it a throwaway copy
    monkeypatch.setattr(
        dataset_mapping_module,
        "dataset_to_resource",
        dict(dataset_mapping_module.dataset_to_resource),
    )
    container.reset("dataset_mapping")
    yield calls
    container.reset("dataset_mapping")


def test_mapping_file_is_read_once_across_all_callers(counting_mapping_file):
    from_streams = streams.get_dataset_mapping()
    from_container = container.get("dataset_mapping")
    from_streams_again = streams.get_dataset_mapping()

    assert from_streams is from_container
    assert from_streams is from_streams_again
    assert counting_mapping_file == ["gs://fake/mapping.tsv"]
    assert from_streams.get_resource_and_version_bytes_by_dataset(b"DS_FAKE") == (
        b"fake_resource",
        b"v1",
    )


def _own_dataset_mapping_constructions(module) -> list[str]:
    """Every place `module`'s source could build a `DatasetMapping` of its own.

    Reports both halves of the pattern this file exists to keep out: naming the class
    (an import of it, at module level or inside a function body) and calling it.
    """
    tree = ast.parse(pathlib.Path(inspect.getsourcefile(module)).read_text())
    found = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == DATASET_MAPPING_MODULE:
            found += [
                f"line {node.lineno}: imports {alias.name}"
                for alias in node.names
                if alias.name == "DatasetMapping"
            ]
        elif isinstance(node, ast.Import):
            found += [
                f"line {node.lineno}: imports {alias.name}"
                for alias in node.names
                if alias.name == DATASET_MAPPING_MODULE
            ]
        elif isinstance(node, ast.Call):
            name = getattr(node.func, "id", None) or getattr(node.func, "attr", None)
            if name == "DatasetMapping":
                found.append(f"line {node.lineno}: constructs DatasetMapping()")
    return found


def test_coloc_constructs_no_dataset_mapping_of_its_own():
    """The coloc module may only ever resolve the shared mapping, never build one.

    Asserted over the module's source rather than by running the code path: the
    statement in question sits behind `_get_resource_access`, which needs tabix files
    off GCS, so no offline test can reach it. What is checked is the property that
    matters and only that — the module names no `DatasetMapping` and calls none — so
    it survives the later cleanup of the dead local this once pinned in place by
    asserting a specific module attribute existed.
    """
    assert _own_dataset_mapping_constructions(data_access_coloc) == []
