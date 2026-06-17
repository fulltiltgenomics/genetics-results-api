"""
Service for serving named curated variant sets (configured in `variant_set_files`).

Each set is a small newline-delimited text file in GCS (one variant id per line, e.g.
`chr1_5045339_C_T`). These back the annotation tool's example/priority variant lists, but the
endpoint is generic — any client can expand a named set into a concrete variant list.
"""

import logging

import app.config.common as config
from app.core.exceptions import NotFoundException, ParseException
from app.core.file_utils import read_file
from app.core.variant import Variant

logger = logging.getLogger(__name__)


class VariantSetService:
    """Reads curated variant-set files and returns canonical variant ids."""

    def list_names(self) -> list[str]:
        """Names of all configured variant sets."""
        return sorted(config.variant_set_files.keys())

    def get_variants(self, name: str) -> list[str]:
        """Return the variants of a named set as canonical `chr:pos:ref:alt` strings.

        Lines are validated through Variant so malformed/header lines are skipped rather than
        served as junk. Raises NotFoundException for an unknown name or a missing file.
        """
        cfg = config.variant_set_files.get(name)
        if cfg is None:
            raise NotFoundException(f"Unknown variant set: {name}")

        path = cfg["file"]
        try:
            content = read_file(path)
        except FileNotFoundError:
            raise NotFoundException(f"Variant set file not found for {name}")
        except Exception as e:
            logger.error(f"Error reading variant set {name} from {path}: {e}")
            raise

        variants: list[str] = []
        for raw in content.splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            try:
                v = Variant(line)
            except ParseException:
                logger.warning(f"skipping unparseable variant '{line}' in set {name}")
                continue
            chr_str = "X" if v.chr == 23 else str(v.chr)
            variants.append(f"{chr_str}:{v.pos}:{v.ref}:{v.alt}")
        return variants
