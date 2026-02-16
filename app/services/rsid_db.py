import re
import sqlite3
import threading
from collections import defaultdict as dd
from typing import Any
from app.core.exceptions import ParseException
from app.core.variant import Variant

RSID_REGEX: re.Pattern[str] = re.compile(r"^rs\d+$", re.IGNORECASE)


class RsidDB:
    def __init__(self, conf: dict[str, Any]) -> None:
        self.rsid_conn: dict[int, sqlite3.Connection] = dd(
            lambda: sqlite3.connect(conf["rsid_db"]["file"])
        )

    def get_variants_by_rsid(self, rsid: str) -> list[Variant]:
        rsid = rsid.lower()
        if RSID_REGEX.match(rsid) is None:
            raise ParseException("invalid rsid")
        if self.rsid_conn[threading.get_ident()].row_factory is None:
            self.rsid_conn[threading.get_ident()].row_factory = sqlite3.Row
        c: sqlite3.Cursor = self.rsid_conn[threading.get_ident()].cursor()
        c.execute("SELECT chr, pos, ref, alt FROM rsid WHERE rsid = ?", (rsid,))
        return [
            Variant(
                row["chr"] + "-" + str(row["pos"]) + "-" + row["ref"] + "-" + row["alt"]
            )
            for row in c.fetchall()
        ]

    def get_variants_by_rsids(self, rsids: list[str]) -> dict[str, list[str]]:
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
        unique_rsids = list(set(normalized))

        conn = self.rsid_conn[threading.get_ident()]
        if conn.row_factory is None:
            conn.row_factory = sqlite3.Row
        c = conn.cursor()

        placeholders = ",".join("?" * len(unique_rsids))
        c.execute(
            f"SELECT chr, pos, ref, alt, rsid FROM rsid WHERE rsid IN ({placeholders})",
            unique_rsids,
        )

        result: dict[str, list[str]] = {r: [] for r in normalized}
        for row in c.fetchall():
            variant_str = f"{row['chr']}:{row['pos']}:{row['ref']}:{row['alt']}"
            result[row["rsid"]].append(variant_str)

        return result
