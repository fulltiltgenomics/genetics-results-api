import re
import sqlite3
import threading
from collections import defaultdict as dd
from typing import Any
from app.core.exceptions import ParseException
from app.core.variant import Variant
from app.core.singleton import Singleton

RSID_REGEX: re.Pattern[str] = re.compile("rs[0-9]+")


class RsidDB(object, metaclass=Singleton):
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
