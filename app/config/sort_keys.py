"""
Sort key utilities for creating dynamic sort functions based on column headers.

This module provides utilities to create sort key functions.
The data in tabix files are sorted by certain columns and these functions are used to merge the files in correct order.
"""

from typing import Callable


def create_sort_key(
    header: list[bytes], sort_config: list[tuple[str, type]]
) -> Callable[[list[bytes]], tuple]:
    """
    Create a sort key function from header and sort configuration.

    This function creates a sort key based on the header present in the data.

    Args:
        header: Actual header with all columns (including any prepended ones like resource/version)
        sort_config: List of (column_name, type) tuples defining sort order and conversions
                    Example: [("chr", int), ("pos", int), ("ref", bytes)]

    Returns:
        Sort key function that can be used with sorted(), heapq.merge(), etc.

    Raises:
        ValueError: If any column in sort_config is not found in header

    Example:
        >>> header = [b"resource", b"version", b"chr", b"pos", b"ref", b"alt"]
        >>> sort_config = [("chr", int), ("pos", int), ("ref", bytes)]
        >>> sort_fn = create_sort_key(header, sort_config)
        >>> line = [b"finngen", b"R13", b"1", b"12345", b"A", b"T"]
        >>> sort_fn(line)
        (1, 12345, b'A')
    """
    indices = []
    types = []

    for col_name, col_type in sort_config:
        try:
            idx = header.index(col_name.encode())
            indices.append(idx)
            types.append(col_type)
        except ValueError:
            raise ValueError(
                f"Column '{col_name}' not found in header. "
                f"Available columns: {[h.decode() for h in header]}"
            )

    def sort_key(x: list[bytes]) -> tuple:
        """Sort key function created from the configuration."""
        return tuple(
            typ(x[idx]) if typ != bytes else x[idx] for idx, typ in zip(indices, types)
        )

    return sort_key


def create_sort_key_from_dict(
    header_with_resources: list[bytes], sort_config_dict: dict[str, str]
) -> Callable[[list[bytes]], tuple]:
    """
    Create a sort key function from a header and a dictionary mapping column names to indices.

    This is an alternative helper for cases where the sort configuration is provided
    as a dictionary with column names as keys (like sort_key_coloc).

    Args:
        header_with_resources: Header list including prepended columns
        sort_config_dict: Dict of column names used for sorting (values are ignored)

    Returns:
        Sort key function

    Example:
        >>> header = [b"resource1", b"version1", b"resource2", b"version2", b"chr", b"region_start_min"]
        >>> config = {"chr": 0, "region_start_min": 1}  # values ignored, only keys matter
        >>> sort_fn = create_sort_key_from_dict(header, config)
    """
    # Extract column names from the dict keys and infer types
    sort_config = []
    for col_name in sort_config_dict.keys():
        # Infer type from column name patterns
        if any(x in col_name for x in ["chr", "pos", "start", "end", "size", "nsnps"]):
            col_type = int
        else:
            col_type = bytes
        sort_config.append((col_name, col_type))

    return create_sort_key(header_with_resources, sort_config)


# these define what columns to merge by
SORT_CONFIG_CS = [
    ("chr", int),
    ("pos", int),
    ("ref", bytes),
    ("alt", bytes),
    ("trait", bytes),
]

SORT_CONFIG_CS_QTL = [
    ("chr", int),
    ("pos", int),
    ("ref", bytes),
    ("alt", bytes),
    ("trait", bytes),
]

SORT_CONFIG_COLOC_CREDSET = [
    ("chr", int),
    ("pos", int),
    ("ref", bytes),
    ("alt", bytes),
    ("trait", bytes),
]

SORT_CONFIG_COLOC = [
    ("chr", int),
    ("region_start_min", int),
    ("region_end_max", int),
    ("trait1", bytes),
    ("trait2", bytes),
]

SORT_CONFIG_COLOC_SIMPLE = [
    ("chr", int),
    ("region_start_min", int),
    ("region_end_max", int),
    ("trait", bytes),
]

SORT_CONFIG_EXPRESSION = [
    ("chrom", int),
    ("gene_start", int),
    ("gene_end", int),
    ("tissue_cell", bytes),
]

SORT_CONFIG_CHROMATIN_PEAKS = [
    ("chrom", int),
    ("start", int),
    ("end", int),
]

SORT_CONFIG_EXOME = [
    ("chr", int),
    ("pos", int),
    ("ref", bytes),
    ("alt", bytes),
    ("trait", bytes),
]
