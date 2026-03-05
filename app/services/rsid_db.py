import re
import asyncio
import logging
import tempfile
from app.core.exceptions import ParseException
from app.core.variant import Variant
from app.services.gcloud_tabix_base import GCloudTabixBase, ensure_gcs_token

logger = logging.getLogger(__name__)

RSID_REGEX: re.Pattern[str] = re.compile(r"^rs\d+$", re.IGNORECASE)
INDEX_CACHE_DIR = "/tmp/tbi_cache"


class RsidDB(GCloudTabixBase):
    def __init__(self, rsid_file: str) -> None:
        super().__init__()
        self.rsid_file = rsid_file

    async def get_variants_by_rsid(self, rsid: str) -> list[Variant]:
        rsid = rsid.lower()
        if RSID_REGEX.match(rsid) is None:
            raise ParseException("invalid rsid")
        rsid_num = rsid[2:]
        ensure_gcs_token()
        process = await asyncio.create_subprocess_exec(
            "tabix",
            "--csi",
            self.rsid_file,
            f"rs:{rsid_num}-{rsid_num}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=INDEX_CACHE_DIR,
        )
        stdout, stderr = await process.communicate()
        if process.returncode != 0:
            logger.error(f"tabix rsid lookup failed: {stderr.decode()}")
            return []
        return self._parse_variants(stdout)

    async def get_variants_by_rsids(self, rsids: list[str]) -> dict[str, list[str]]:
        """Batch lookup of variants by rsids.

        Args:
            rsids: List of rsids (must be pre-validated)

        Returns:
            Dict mapping each rsid (lowercased) to list of variant strings
            in format "chr:pos:ref:alt". If rsid not found, returns empty list.
        """
        if not rsids:
            return {}

        normalized = [r.lower() for r in rsids]
        result: dict[str, list[str]] = {r: [] for r in normalized}
        unique_rsids = list(set(normalized))

        regions = []
        for rsid in unique_rsids:
            rsid_num = rsid[2:]
            regions.append(f"rs\t{rsid_num}\t{rsid_num}")

        ensure_gcs_token()
        with tempfile.NamedTemporaryFile(mode="w", suffix=".regions") as tmp:
            tmp.write("\n".join(regions))
            tmp.flush()
            process = await asyncio.create_subprocess_exec(
                "tabix",
                "--csi",
                "-R",
                tmp.name,
                self.rsid_file,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=INDEX_CACHE_DIR,
            )
            stdout, stderr = await process.communicate()

        if process.returncode != 0:
            logger.error(f"tabix rsid batch lookup failed: {stderr.decode()}")
            return result

        for line in stdout.decode().strip().split("\n"):
            if not line:
                continue
            fields = line.split("\t")
            # format: chr(rs), pos(rsid_num), rsid, real_chr, real_pos, real_ref, real_alt
            rsid = fields[2]
            variant_str = f"{fields[3]}:{fields[4]}:{fields[5]}:{fields[6]}"
            if rsid in result:
                result[rsid].append(variant_str)

        return result

    def _parse_variants(self, stdout: bytes) -> list[Variant]:
        variants = []
        for line in stdout.decode().strip().split("\n"):
            if not line:
                continue
            fields = line.split("\t")
            # format: chr(rs), pos(rsid_num), rsid, real_chr, real_pos, real_ref, real_alt
            variants.append(
                Variant(f"{fields[3]}-{fields[4]}-{fields[5]}-{fields[6]}")
            )
        return variants
