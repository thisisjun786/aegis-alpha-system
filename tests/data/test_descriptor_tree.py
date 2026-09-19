# ruff: noqa: SLF001
from __future__ import annotations

import os
import sys
import tracemalloc
from pathlib import Path
from typing import Literal

import pytest

from aegis_alpha.data.data_root import (
    BoundDataPath,
    DataRootCapability,
    DataRootError,
    open_data_root_capability,
    resolve_data_root,
)
from aegis_alpha.data.descriptor_tree import (
    DescriptorTree,
    DescriptorTreeError,
    NorgateRuntimeIO,
)


def test_atomic_bytes_and_cross_tree_rename_are_descriptor_relative(tmp_path: Path) -> None:
    left = tmp_path / "left"
    right = tmp_path / "right"
    left.mkdir()
    right.mkdir()
    with DescriptorTree.open_path(left) as source, DescriptorTree.open_path(right) as target:
        source.mkdir("staging")
        source.atomic_write_bytes("staging/payload", b"candidate")
        assert source.read_bytes("staging/payload") == b"candidate"
        source.rename_to("staging", target, "published")
        assert target.read_bytes("published/payload") == b"candidate"
        assert not source.exists("staging")


def test_open_tree_keeps_writes_out_of_replacement_ancestor(tmp_path: Path) -> None:
    visible = tmp_path / "visible"
    visible.mkdir()
    with DescriptorTree.open_path(visible) as tree:
        original = tmp_path / "original"
        visible.rename(original)
        visible.mkdir()
        (visible / "sentinel").write_bytes(b"replacement")

        tree.atomic_write_bytes("owned", b"old-tree")

    assert (original / "owned").read_bytes() == b"old-tree"
    assert (visible / "sentinel").read_bytes() == b"replacement"
    assert not (visible / "owned").exists()


def test_symlink_and_fifo_leaves_are_rejected_without_blocking(tmp_path: Path) -> None:
    root = tmp_path / "root"
    outside = tmp_path / "outside"
    root.mkdir()
    outside.write_bytes(b"outside")
    (root / "alias").symlink_to(outside)
    os.mkfifo(root / "fifo")

    with DescriptorTree.open_path(root) as tree:
        with pytest.raises(DescriptorTreeError), tree.binary_reader("alias"):
            pass
        with pytest.raises(DescriptorTreeError, match="regular file"), tree.binary_reader("fifo"):
            pass


def test_owned_cleanup_preserves_a_replacement_tree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "root"
    target = root / "target"
    target.mkdir(parents=True)
    (target / "owned").write_bytes(b"owned")
    original_remove = DescriptorTree._remove_contents.__func__  # type: ignore[attr-defined]
    replaced = False

    def replace_after_cleanup(cls: type[DescriptorTree], descriptor: int) -> None:
        nonlocal replaced
        original_remove(cls, descriptor)
        if not replaced:
            replaced = True
            target.rename(root / "detached-owned")
            target.mkdir()
            (target / "sentinel").write_bytes(b"replacement")

    monkeypatch.setattr(DescriptorTree, "_remove_contents", classmethod(replace_after_cleanup))
    with DescriptorTree.open_path(root) as tree:
        expected = tree.directory_identity("target")
        with pytest.raises(DescriptorTreeError, match="replaced during cleanup"):
            tree.remove_tree("target", expected=expected)

    assert (target / "sentinel").read_bytes() == b"replacement"
    assert not (root / "detached-owned" / "owned").exists()


def test_runtime_context_detects_source_and_output_parent_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = (tmp_path / "data").resolve()
    snapshot = root / "raw" / "snapshot"
    output_parent = root / "normalized" / "publication"
    snapshot.mkdir(parents=True)
    output_parent.mkdir(parents=True)
    (snapshot / "manifest.json").write_bytes(b"original")
    monkeypatch.setenv("AAS_DATA_ROOT", os.fspath(root))
    data_root = resolve_data_root()

    with open_data_root_capability(data_root) as capability:
        source = capability.bind_path(snapshot, namespace="raw")
        output = capability.bind_path(
            output_parent / "dataset",
            namespace="normalized",
            allow_missing_final=True,
        )
        with NorgateRuntimeIO(capability, source, output) as runtime:
            old_snapshot = snapshot.with_name("snapshot-old")
            snapshot.rename(old_snapshot)
            snapshot.mkdir()
            (snapshot / "manifest.json").write_bytes(b"replacement")

            with pytest.raises(DataRootError, match="replaced"):
                runtime.revalidate()
            assert runtime.source.read_bytes("manifest.json") == b"original"
            assert (snapshot / "manifest.json").read_bytes() == b"replacement"


@pytest.mark.parametrize("target_kind", ["source", "output-parent"])
def test_bound_tree_open_rejects_replacement_after_revalidation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    target_kind: str,
) -> None:
    root = (tmp_path / "data").resolve()
    snapshot = root / "raw" / "snapshot"
    output_parent = root / "normalized" / "publication"
    snapshot.mkdir(parents=True)
    output_parent.mkdir(parents=True)
    monkeypatch.setenv("AAS_DATA_ROOT", os.fspath(root))
    data_root = resolve_data_root()

    with open_data_root_capability(data_root) as capability:
        source = capability.bind_path(snapshot, namespace="raw")
        output = capability.bind_path(
            output_parent / "dataset",
            namespace="normalized",
            allow_missing_final=True,
        )
        target_binding = source if target_kind == "source" else output
        target = snapshot if target_kind == "source" else output_parent
        detached = target.with_name(f"{target.name}-detached")
        original_revalidate = DataRootCapability.revalidate_path
        armed = True

        def replace_after_revalidation(
            active: DataRootCapability,
            binding: BoundDataPath,
        ) -> None:
            nonlocal armed
            original_revalidate(active, binding)
            if active is capability and binding is target_binding and armed:
                armed = False
                target.rename(detached)
                target.mkdir()
                (target / "sentinel").write_bytes(b"replacement")

        monkeypatch.setattr(DataRootCapability, "revalidate_path", replace_after_revalidation)
        opener = (
            capability.open_bound_tree
            if target_kind == "source"
            else capability.open_bound_parent_tree
        )
        binding = source if target_kind == "source" else output
        with pytest.raises(DataRootError, match="replaced while being reopened"):
            opener(binding)

    assert (target / "sentinel").read_bytes() == b"replacement"


def test_published_output_open_rejects_leaf_replacement_after_revalidation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = (tmp_path / "data").resolve()
    snapshot = root / "raw" / "snapshot"
    output_parent = root / "normalized" / "publication"
    output = output_parent / "dataset"
    snapshot.mkdir(parents=True)
    output.mkdir(parents=True)
    (output / "admitted").write_bytes(b"old")
    monkeypatch.setenv("AAS_DATA_ROOT", os.fspath(root))
    data_root = resolve_data_root()

    with open_data_root_capability(data_root) as capability:
        source_binding = capability.bind_path(snapshot, namespace="raw")
        output_binding = capability.bind_path(output, namespace="normalized")
        with NorgateRuntimeIO(capability, source_binding, output_binding) as runtime:
            original_subtree = DescriptorTree.subtree
            armed = True

            def replace_before_open(
                tree: DescriptorTree,
                relative: str | os.PathLike[str],
            ) -> DescriptorTree:
                nonlocal armed
                if tree is runtime.output_parent and os.fspath(relative) == "dataset" and armed:
                    armed = False
                    output.rename(output_parent / "dataset-detached")
                    output.mkdir()
                    (output / "sentinel").write_bytes(b"replacement")
                return original_subtree(tree, relative)

            monkeypatch.setattr(DescriptorTree, "subtree", replace_before_open)
            with pytest.raises(DescriptorTreeError, match="replaced while being pinned"):
                runtime.published_output()

    assert (output / "sentinel").read_bytes() == b"replacement"
    assert not (output / "admitted").exists()


def test_published_output_binding_rejects_parent_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = (tmp_path / "data").resolve()
    snapshot = root / "raw" / "snapshot"
    output_parent = root / "normalized" / "publication"
    output = output_parent / "dataset"
    snapshot.mkdir(parents=True)
    output_parent.mkdir(parents=True)
    monkeypatch.setenv("AAS_DATA_ROOT", os.fspath(root))
    data_root = resolve_data_root()

    with open_data_root_capability(data_root) as capability:
        source_binding = capability.bind_path(snapshot, namespace="raw")
        output_binding = capability.bind_path(
            output,
            namespace="normalized",
            allow_missing_final=True,
        )
        with NorgateRuntimeIO(capability, source_binding, output_binding) as runtime:
            runtime.output_parent.mkdir("dataset")
            runtime.output_parent.atomic_write_bytes("dataset/admitted", b"old")
            expected = runtime.output_parent.directory_identity("dataset")
            original_bind = DataRootCapability.bind_path
            armed = True

            def replace_before_visible_binding(
                active: DataRootCapability,
                path: Path,
                *,
                namespace: Literal["raw", "normalized"],
                allow_missing_final: bool = False,
                create_parents: bool = False,
            ) -> BoundDataPath:
                nonlocal armed
                if active is capability and path == output and armed:
                    armed = False
                    output_parent.rename(root / "normalized" / "publication-detached")
                    output.mkdir(parents=True)
                    (output / "sentinel").write_bytes(b"replacement")
                return original_bind(
                    active,
                    path,
                    namespace=namespace,
                    allow_missing_final=allow_missing_final,
                    create_parents=create_parents,
                )

            monkeypatch.setattr(DataRootCapability, "bind_path", replace_before_visible_binding)
            with pytest.raises(DescriptorTreeError, match="parent was replaced"):
                runtime.bind_published_output(expected=expected)

    assert (output / "sentinel").read_bytes() == b"replacement"
    assert not (output / "admitted").exists()


def test_new_publication_rejects_leaf_replacement_before_first_open(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = (tmp_path / "data").resolve()
    snapshot = root / "raw" / "snapshot"
    output_parent = root / "normalized" / "publication"
    output = output_parent / "dataset"
    snapshot.mkdir(parents=True)
    output_parent.mkdir(parents=True)
    monkeypatch.setenv("AAS_DATA_ROOT", os.fspath(root))
    data_root = resolve_data_root()

    with open_data_root_capability(data_root) as capability:
        source_binding = capability.bind_path(snapshot, namespace="raw")
        output_binding = capability.bind_path(
            output,
            namespace="normalized",
            allow_missing_final=True,
        )
        with NorgateRuntimeIO(capability, source_binding, output_binding) as runtime:
            runtime.output_parent.mkdir("dataset")
            runtime.output_parent.atomic_write_bytes("dataset/admitted", b"old")
            expected = runtime.output_parent.directory_identity("dataset")
            original_subtree = DescriptorTree.subtree
            armed = True

            def replace_before_first_open(
                tree: DescriptorTree,
                relative: str | os.PathLike[str],
            ) -> DescriptorTree:
                nonlocal armed
                if tree is runtime.output_parent and os.fspath(relative) == "dataset" and armed:
                    armed = False
                    output.rename(output_parent / "dataset-detached")
                    output.mkdir()
                    (output / "sentinel").write_bytes(b"replacement")
                return original_subtree(tree, relative)

            monkeypatch.setattr(DescriptorTree, "subtree", replace_before_first_open)
            with pytest.raises(DescriptorTreeError, match="differs from staged dataset"):
                runtime.bind_published_output(expected=expected)

    assert (output / "sentinel").read_bytes() == b"replacement"
    assert not (output / "admitted").exists()


def test_closed_descriptor_tree_refuses_use(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    tree = DescriptorTree.open_path(root)
    descriptor = tree.descriptor
    tree.close()
    tree.close()
    with pytest.raises(DescriptorTreeError, match="closed"):
        tree.listdir()
    with pytest.raises(OSError, match="Bad file descriptor"):
        os.fstat(descriptor)


def test_relative_path_components_are_never_interned(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A caller's content identifier must not be inserted into the interned table.

    pathlib interns every component it parses, so routing content-addressed names
    through it inserted each distinct digest into one process-global dictionary.
    The insertion that crosses that dictionary's next doubling threshold allocates
    the whole new keys table at the calling frame: 3,844,800 bytes at the 2**18
    step, which any allocation measurement open at that moment is charged for.
    Interning is observed here as it happens, because 3.13 interns mortally and a
    component whose last reference dies leaves the table again.
    """
    interned: list[str] = []
    original_intern = sys.intern

    def recording_intern(value: str) -> str:
        interned.append(value)
        return original_intern(value)

    root = tmp_path / "root"
    root.mkdir()
    # Built at runtime so no compile-time literal is interned on its behalf.
    digest = "".join(f"{index % 10}" for index in range(64))
    relative = digest[:2] + "/" + digest
    with DescriptorTree.open_path(root) as tree:
        # Recording starts after the tree is open: opening parses the caller's own
        # absolute root, which is one fixed installation path rather than a fresh
        # identifier per stored object.
        monkeypatch.setattr(sys, "intern", recording_intern)
        tree.mkdir(digest[:2])
        tree.atomic_write_bytes(relative, b"content")
        assert tree.read_bytes(relative) == b"content"
        assert tree.exists(relative)
        assert tree.stat(relative).st_size == len(b"content")
    assert interned == []


def test_capped_read_allocates_within_the_cap_it_was_given(tmp_path: Path) -> None:
    """The executed read must stay inside the size the caller approved.

    Decision 0016 requires approval before reading. A fixed megabyte request
    allocated that megabyte whatever the cap said, so a 128 KiB admission paid
    eight times its approved charge and dominated every allocation measurement
    taken around it.
    """
    root = tmp_path / "root"
    root.mkdir()
    cap = 8 * 1024
    stored_bytes = 4096
    with DescriptorTree.open_path(root) as tree:
        tree.atomic_write_bytes("payload", b"x" * stored_bytes)
        tracemalloc.start()
        try:
            assert len(tree.read_bytes("payload", max_bytes=cap)) == stored_bytes
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
    assert peak < cap + 64 * 1024


@pytest.mark.parametrize("size", [0, 1, 4096])
def test_exact_cap_is_admitted_and_one_byte_more_is_refused(tmp_path: Path, size: int) -> None:
    root = tmp_path / "root"
    root.mkdir()
    with DescriptorTree.open_path(root) as tree:
        tree.atomic_write_bytes("payload", b"y" * size)
        assert tree.read_bytes("payload", max_bytes=size) == b"y" * size
        tree.atomic_write_bytes("payload", b"y" * (size + 1))
        with pytest.raises(DescriptorTreeError, match="exceeds size cap"):
            tree.read_bytes("payload", max_bytes=size)


def test_negative_read_cap_is_refused_rather_than_read_as_empty(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    with DescriptorTree.open_path(root) as tree:
        tree.atomic_write_bytes("payload", b"content")
        with pytest.raises(DescriptorTreeError, match="must not be negative"):
            tree.read_bytes("payload", max_bytes=-1)
