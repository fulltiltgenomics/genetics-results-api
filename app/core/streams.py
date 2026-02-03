import logging
from typing import AsyncGenerator, AsyncIterator, Any, Callable
import asyncio

from app.core.logging_config import setup_logging
from app.core.variant import Variant
from app.services.dataset_mapping import DatasetMapping


setup_logging()
logger = logging.getLogger(__name__)

dataset_mapping = DatasetMapping()


async def tsv_line_iterator_base(
    stream: AsyncIterator[bytes],
    filter_fn: Callable[[list[bytes]], bool],
    transform_fn: Callable[[list[bytes]], list[bytes]],
) -> AsyncIterator[list[bytes]]:
    """
    Generic TSV line iterator with customizable filtering and transformation.

    This base function handles the common pattern of:
    1. Buffering incomplete lines from stream chunks
    2. Splitting lines into tab-separated fields
    3. Filtering lines based on custom criteria
    4. Transforming lines before yielding

    Args:
        stream: Async iterator of byte chunks
        filter_fn: Function that takes split line and returns True to keep, False to skip
        transform_fn: Function that takes split line and returns transformed line

    Yields:
        Transformed lines that pass the filter
    """
    buffer = b""  # buffer for possibly incomplete lines

    async for chunk in stream:
        data = buffer + chunk
        lines = data.split(b"\n")

        # process all except the last line, which might be incomplete
        for line in lines[:-1]:
            if line.strip() != b"":
                s = line.split(b"\t")

                if not filter_fn(s):
                    continue

                yield transform_fn(s)

        # keep the last line in buffer
        buffer = lines[-1]

    # process final buffer if it contains data
    if buffer.strip() != b"":
        s = buffer.split(b"\t")
        if filter_fn(s):
            yield transform_fn(s)


def tsv_line_iterator_simple(
    stream: AsyncIterator[bytes],
    header: list[bytes],
    columns: dict[str, bytes],
) -> AsyncIterator[list[bytes]]:
    """
    Simple TSV line iterator that only adds resource/version columns.
    Use this for data types that don't need variant filtering (e.g., expression data).

    Args:
        stream: Async iterator of byte chunks
        header: Header columns from the data file (used to look up column indices)
        columns: Dict mapping column keys to column names (e.g. {"dataset": b"dataset"})
    """
    # derive column index from header
    dataset_col = header.index(columns["dataset"])

    def filter_fn(s: list[bytes]) -> bool:
        return True

    def transform_fn(s: list[bytes]) -> list[bytes]:
        resource_bytes, version_bytes = (
            dataset_mapping.get_resource_and_version_bytes_by_dataset(s[dataset_col])
        )
        return [resource_bytes, version_bytes] + s

    return tsv_line_iterator_base(stream, filter_fn, transform_fn)


def tsv_line_iterator(
    stream: AsyncIterator[bytes],
    header: list[bytes],
    columns: dict[str, bytes],
    variant: Variant | None,
) -> AsyncIterator[list[bytes]]:
    """
    Iterate over lines in a stream and split them into a list.
    Adds resource and version as the first two columns.
    If variant is provided, limit data to the variant.

    Args:
        stream: Async iterator of byte chunks
        header: Header columns from the data file (used to look up column indices)
        columns: Dict mapping column keys to column names (e.g. {"chr": b"chr", ...})
        variant: Variant to filter by, or None for no filtering
    """
    # derive column indices from header
    chr_col = header.index(columns["chr"])
    pos_col = header.index(columns["pos"])
    ref_col = header.index(columns["ref"])
    alt_col = header.index(columns["alt"])
    dataset_col = header.index(columns["dataset"])

    def filter_fn(s: list[bytes]) -> bool:
        """Filter to specific variant if provided."""
        if variant is None:
            return True
        return (
            variant.chr_bytes == s[chr_col]
            and variant.pos_bytes == s[pos_col]
            and variant.ref_bytes == s[ref_col]
            and variant.alt_bytes == s[alt_col]
        )

    def transform_fn(s: list[bytes]) -> list[bytes]:
        """Add resource and version columns."""
        resource_bytes, version_bytes = (
            dataset_mapping.get_resource_and_version_bytes_by_dataset(s[dataset_col])
        )
        return [resource_bytes, version_bytes] + s

    return tsv_line_iterator_base(stream, filter_fn, transform_fn)


def tsv_line_iterator_qtl(
    stream: AsyncIterator[bytes],
    header: list[bytes],
    columns: dict[str, bytes],
    start_positions: list[int],
    end_positions: list[int],
) -> AsyncIterator[list[bytes]]:
    """
    Iterate over lines in a stream and split them into a list.
    Adds resource and version as the first two columns.
    Filters to specific gene start/end positions.

    Args:
        stream: Async iterator of byte chunks
        header: Header columns from the data file (used to look up column indices)
        columns: Dict mapping column keys to column names (e.g. {"trait_start": b"trait_start", ...})
        start_positions: List of gene start positions to filter by
        end_positions: List of gene end positions to filter by
    """
    # derive column indices from header
    trait_start_col = header.index(columns["trait_start"])
    trait_end_col = header.index(columns["trait_end"])
    dataset_col = header.index(columns["dataset"])

    # pre-compute position pairs as bytes for efficient comparison
    start_end_positions_bytes = [
        str(start_position).encode() + b"\t" + str(end_position).encode()
        for start_position, end_position in zip(start_positions, end_positions)
    ]

    def filter_fn(s: list[bytes]) -> bool:
        """Filter to specific gene positions."""
        # match to both gene start and end positions to match to a specific gene
        return (
            s[trait_start_col] + b"\t" + s[trait_end_col]
            in start_end_positions_bytes
        )

    def transform_fn(s: list[bytes]) -> list[bytes]:
        """Add resource and version columns."""
        resource_bytes, version_bytes = (
            dataset_mapping.get_resource_and_version_bytes_by_dataset(s[dataset_col])
        )
        return [resource_bytes, version_bytes] + s

    return tsv_line_iterator_base(stream, filter_fn, transform_fn)


def tsv_line_iterator_coloc_credset(
    stream: AsyncIterator[bytes],
    header: list[bytes],
    cs_ids: list[bytes],
) -> AsyncIterator[list[bytes]]:
    """
    Iterate over lines in a stream and split them into a list.
    Filter to given cs_ids (dataset|trait|cs_id).
    """

    # Get column indices from header
    dataset_index = header.index(b"dataset")
    trait_index = header.index(b"trait")
    cs_id_index = header.index(b"cs_id")

    def filter_fn(s: list[bytes]) -> bool:
        """Filter to specific credible set IDs."""
        cs_id = s[dataset_index] + b"|" + s[trait_index] + b"|" + s[cs_id_index]
        return cs_id in cs_ids

    def transform_fn(s: list[bytes]) -> list[bytes]:
        """No transformation needed."""
        return s

    return tsv_line_iterator_base(stream, filter_fn, transform_fn)


def tsv_line_iterator_coloc(
    stream: AsyncIterator[bytes],
    header: list[bytes],
    cs_ids: list[bytes],
) -> AsyncIterator[list[bytes]]:
    """
    Iterate over lines in a stream and split them into a list.
    Filter to given cs_ids (dataset1|trait1|cs1_id or dataset2|trait2|cs2_id).
    Adds dual resource/version columns for both datasets.
    """

    # Get column indices from header
    dataset1_index = header.index(b"dataset1")
    dataset2_index = header.index(b"dataset2")
    trait1_index = header.index(b"trait1")
    trait2_index = header.index(b"trait2")
    cs1_id_index = header.index(b"cs1_id")
    cs2_id_index = header.index(b"cs2_id")

    def filter_fn(s: list[bytes]) -> bool:
        """Filter to lines where either cs1 or cs2 is in the target set."""
        cs1_id = s[dataset1_index] + b"|" + s[trait1_index] + b"|" + s[cs1_id_index]
        cs2_id = s[dataset2_index] + b"|" + s[trait2_index] + b"|" + s[cs2_id_index]
        return cs1_id in cs_ids or cs2_id in cs_ids

    def transform_fn(s: list[bytes]) -> list[bytes]:
        """Add dual resource and version columns."""
        dataset1 = s[dataset1_index]
        dataset2 = s[dataset2_index]
        resource1, version1 = dataset_mapping.get_resource_and_version_bytes_by_dataset(
            dataset1
        )
        resource2, version2 = dataset_mapping.get_resource_and_version_bytes_by_dataset(
            dataset2
        )
        return [resource1, version1, resource2, version2] + s

    return tsv_line_iterator_base(stream, filter_fn, transform_fn)


def tsv_line_iterator_coloc_by_trait(
    stream: AsyncIterator[bytes],
    header: list[bytes],
    cs_ids: list[bytes],
    simple: bool = False,
) -> AsyncIterator[list[bytes]]:
    """
    Iterate over lines in a stream and split them into a list.
    Filter to given cs_ids (trait1|cs1_id or trait2|cs2_id or dataset1|cs1_id or dataset2|cs2_id).
    Adds dual resource/version columns for both datasets.
    """

    cs_identifier_parts = cs_ids[0].split(b"|")
    resource = cs_identifier_parts[0]
    trait = cs_identifier_parts[1] if len(cs_identifier_parts) > 1 else b""

    dataset1_index = header.index(b"dataset1")
    dataset2_index = header.index(b"dataset2")
    trait1_index = header.index(b"trait1")
    trait2_index = header.index(b"trait2")
    cs1_id_index = header.index(b"cs1_id")
    cs2_id_index = header.index(b"cs2_id")

    def _get_index_optional(column_name: bytes) -> int | None:
        try:
            return header.index(column_name)
        except ValueError:
            return None

    paired_column_indices: list[tuple[int, int]] = [(dataset1_index, dataset2_index)]
    for left, right in [
        (b"data_type1", b"data_type2"),
        (b"trait1", b"trait2"),
        (b"trait1_original", b"trait2_original"),
        (b"cell_type1", b"cell_type2"),
        (b"cs1_id", b"cs2_id"),
        (b"hit1", b"hit2"),
        (b"hit1_beta", b"hit2_beta"),
        (b"hit1_mlog10p", b"hit2_mlog10p"),
        (b"PP.H1.abf", b"PP.H2.abf"),  # swap H1 (trait1 only) with H2 (trait2 only)
        (b"nsnps1", b"nsnps2"),
        (b"cs1_log10bf", b"cs2_log10bf"),
        (b"cs1_size", b"cs2_size"),
    ]:
        left_idx = _get_index_optional(left)
        right_idx = _get_index_optional(right)
        if left_idx is not None and right_idx is not None:
            paired_column_indices.append((left_idx, right_idx))

    def filter_fn(s: list[bytes]) -> bool:
        """Filter to lines matching any of the four possible cs_id patterns."""
        dataset1 = s[dataset1_index]
        dataset2 = s[dataset2_index]
        trait1 = s[trait1_index]
        trait2 = s[trait2_index]
        resource1, version1 = dataset_mapping.get_resource_and_version_bytes_by_dataset(
            dataset1
        )
        resource2, version2 = dataset_mapping.get_resource_and_version_bytes_by_dataset(
            dataset2
        )
        resource1_dataset1_cs1 = resource1 + b"|" + dataset1 + b"|" + s[cs1_id_index]
        resource2_dataset2_cs2 = resource2 + b"|" + dataset2 + b"|" + s[cs2_id_index]
        resource1_trait1_cs1 = resource1 + b"|" + trait1 + b"|" + s[cs1_id_index]
        resource2_trait2_cs2 = resource2 + b"|" + trait2 + b"|" + s[cs2_id_index]

        return (
            resource1_dataset1_cs1 in cs_ids
            or resource2_dataset2_cs2 in cs_ids
            or resource1_trait1_cs1 in cs_ids
            or resource2_trait2_cs2 in cs_ids
        )

    def _is_query_side(side_resource: bytes, side_trait: bytes) -> bool:
        if side_resource != resource:
            return False
        if trait and side_trait == trait:
            return True
        return False

    def transform_fn(s: list[bytes]) -> list[bytes]:
        """Add dual resource and version columns."""
        dataset1 = s[dataset1_index]
        dataset2 = s[dataset2_index]
        resource1, version1 = dataset_mapping.get_resource_and_version_bytes_by_dataset(
            dataset1
        )
        resource2, version2 = dataset_mapping.get_resource_and_version_bytes_by_dataset(
            dataset2
        )
        if simple:
            side2_matches = _is_query_side(resource2, s[trait2_index])

            # swap the columns if the query side is the second side
            if side2_matches:
                for idx1, idx2 in paired_column_indices:
                    s[idx1], s[idx2] = s[idx2], s[idx1]
                dataset1 = s[dataset1_index]
                dataset2 = s[dataset2_index]
                resource1, resource2 = resource2, resource1
                version1, version2 = version2, version1

            # use singular names (resource, version instead of resource2, version2)
            # also filter out all columns with "1" suffix (trait1, dataset1, cs1_id, etc.)
            def should_keep_col(col_name: bytes) -> bool:
                """Keep columns that don't have '1' suffix or '1_' pattern."""
                if b"1_" in col_name:
                    return False
                if col_name.endswith(b"1"):
                    return False
                return True

            # filter out columns with "1" suffix
            filtered_s = [
                val for idx, val in enumerate(s) if should_keep_col(header[idx])
            ]
            return [resource2, version2] + filtered_s
        else:
            return [resource1, version1, resource2, version2] + s

    return tsv_line_iterator_base(stream, filter_fn, transform_fn)


def tsv_line_iterator_chromatin_peaks(
    stream: AsyncIterator[bytes],
    peak_id: str,
    resource: str,
    version: str,
) -> AsyncIterator[list[bytes]]:
    """
    Iterate over lines in a stream and split them into a list.
    Filter to specific peak_id and prepend resource and version columns.

    Args:
        stream: Async iterator of byte chunks
        peak_id: Peak ID to filter by (e.g., "chr1-817095-817594")
        resource: Resource name to prepend
        version: Version to prepend

    Yields:
        Lines matching the peak_id with resource and version prepended
    """
    peak_id_bytes = peak_id.encode("utf-8")
    resource_bytes = resource.encode("utf-8")
    version_bytes = version.encode("utf-8")

    peak_id_col_index = 3
    chr_col_index = 0
    cell_type_col_index = 6

    def filter_fn(s: list[bytes]) -> bool:
        """Filter to lines matching the peak_id."""
        return len(s) > peak_id_col_index and s[peak_id_col_index] == peak_id_bytes

    def transform_fn(s: list[bytes]) -> list[bytes]:
        """Prepend resource and version columns."""
        s[chr_col_index] = (
            s[chr_col_index]
            .replace(b"chr", b"")
            .replace(b"X", b"23")
            .replace(b"Y", b"24")
            .replace(b"MT", b"26")
        )
        # TODO this is not very clean
        s[cell_type_col_index] = s[cell_type_col_index].replace(
            b"predicted.celltype.", b""
        )
        return [resource_bytes, version_bytes] + s

    return tsv_line_iterator_base(stream, filter_fn, transform_fn)


async def tsv_line_iterator_str(
    stream: AsyncIterator[bytes],
) -> AsyncIterator[list[str]]:
    """Iterate over lines in a stream and split them into a list of strings."""
    buffer = b""  # buffer for possibly incomplete lines

    async for chunk in stream:
        data = buffer + chunk
        lines = data.split(b"\n")
        for line in lines[:-1]:
            if line.strip() != b"":
                yield line.decode().split("\t")
        buffer = lines[-1]

    if buffer.strip() != b"":
        yield buffer.decode().split("\t")


async def tsv_stream_to_list(
    line_stream: AsyncIterator[list[str]],
    header_schema: dict[str, type],
) -> list[dict[str, Any]]:
    """Convert a stream of TSV lines to a list of dictionaries."""
    header = [h[1:] if h.startswith("#") else h for h in await anext(line_stream)]

    # validate header against schema
    for h in header:
        if h not in header_schema:
            raise ValueError(f"Header {h} not found in header schema")

    rows = []
    try:
        async for fields in line_stream:
            row = {}
            for i, field in enumerate(fields):
                # cast value to the correct type
                type_ = header_schema[header[i]]
                if field == "NA":
                    row[header[i]] = None
                elif type_ == bool:
                    row[header[i]] = (
                        field.lower() == "true" or field == "1" or field == "1.0"
                    )
                elif type_ == float:
                    # normalize infinite to 1e308 for JSON compliance
                    if field.lower().startswith("inf"):
                        row[header[i]] = 1e308
                    else:
                        row[header[i]] = float(field)
                else:
                    row[header[i]] = type_(field)
            rows.append(row)
    except Exception as e:
        logger.error(f"Error parsing data: {e}")
        raise ValueError(f"Error parsing data")
    return rows


async def filter_stream_by_cs_id(
    stream: AsyncGenerator[bytes, None],
    target_cs_id: str,
    cs_id_column_index: int = 13,
) -> AsyncGenerator[bytes, None]:
    """
    Filter a TSV byte stream to only rows matching the target cs_id.
    Yields header line and matching data lines.

    Args:
        stream: Async generator yielding byte chunks
        target_cs_id: The cs_id value to filter by
        cs_id_column_index: Column index for cs_id (default 13)
    """
    target_cs_id_bytes = target_cs_id.encode("utf-8")
    buffer = b""
    first_line = True

    async for chunk in stream:
        data = buffer + chunk
        lines = data.split(b"\n")

        for line in lines[:-1]:
            if line.strip() == b"":
                continue

            if first_line:
                # always yield header
                yield line + b"\n"
                first_line = False
                continue

            fields = line.split(b"\t")
            if len(fields) > cs_id_column_index and fields[cs_id_column_index] == target_cs_id_bytes:
                yield line + b"\n"

        buffer = lines[-1]

    # process remaining buffer
    if buffer.strip() != b"":
        fields = buffer.split(b"\t")
        if len(fields) > cs_id_column_index and fields[cs_id_column_index] == target_cs_id_bytes:
            yield buffer + b"\n"


async def chunk_iterator(
    line_iterator: AsyncIterator[list[bytes]],
    header_line: bytes,
    out_chunk_size: int,
) -> AsyncGenerator[bytes, None]:
    buffer = []
    buffer_size_bytes = 0
    max_buffer_size = out_chunk_size

    yield header_line
    async for line in line_iterator:
        line_bytes = b"\t".join(line) + b"\n"
        buffer.append(line_bytes)
        buffer_size_bytes += len(line_bytes)

        if buffer_size_bytes >= max_buffer_size:
            yield b"".join(buffer)
            buffer.clear()
            buffer_size_bytes = 0

    if buffer:
        yield b"".join(buffer)
