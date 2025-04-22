import sqlite3
import threading
from collections import OrderedDict as od, defaultdict as dd
from typing import Any

from app.core.exceptions import DataException
from app.core.singleton import Singleton


class Metadata(object, metaclass=Singleton):
    def __init__(self, conf: dict[str, Any]) -> None:
        self.conf = conf
        self.rsid_conn: dict[int, sqlite3.Connection] = dd(
            lambda: sqlite3.connect(conf["metadata_db"])
        )

    def _dict_factory(self, cursor: sqlite3.Cursor, row: sqlite3.Row) -> dict[str, str]:
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d

    def get_dataset(self, dataset: str) -> dict[str, str | int | None] | None:
        if self.rsid_conn[threading.get_ident()].row_factory is None:
            self.rsid_conn[threading.get_ident()].row_factory = self._dict_factory
        c: sqlite3.Cursor = self.rsid_conn[threading.get_ident()].cursor()
        c.execute(
            """
            SELECT resource, data_type, dataset_id, study_id, study_label,
                   sample_group, tissue_id, tissue_label, condition_label,
                   sample_size, quant_method
            FROM dataset
            WHERE dataset_id = ?
            """,
            (dataset,),
        )
        rows: list[dict[str, str | int | None]] = c.fetchall()
        if len(rows) == 0:
            return None
        return rows[0]

    def get_phenotype(
        self, data_type: str, resource: str, dataset: str, phenocode: str
    ) -> dict[str, str | int | None]:
        if self.rsid_conn[threading.get_ident()].row_factory is None:
            self.rsid_conn[threading.get_ident()].row_factory = self._dict_factory
        c: sqlite3.Cursor = self.rsid_conn[threading.get_ident()].cursor()
        c.execute(
            """
            SELECT resource, data_type, trait_type, phenocode, phenostring, category,
                   chromosome, gene_start, gene_end, strand,
                   num_samples, num_cases, num_controls,
                   pub_author, pub_date
            FROM trait
            WHERE resource = ? AND phenocode = ?
            """,
            (
                resource,
                phenocode,
            ),
        )
        rows: list[dict[str, str | int | None]] = c.fetchall()
        if len(rows) == 0:
            # TODO insert NA into metadata db?
            if phenocode == "NA":
                rows = [
                    od(
                        [
                            ("resource", resource),
                            ("data_type", "NA"),
                            ("trait_type", "NA"),
                            ("phenocode", "NA"),
                            ("phenostring", "NA"),
                            ("category", None),
                            ("chromosome", None),
                            ("gene_start", None),
                            ("gene_end", None),
                            ("strand", None),
                            ("num_samples", 0),
                            ("num_cases", 0),
                            ("num_controls", 0),
                            ("pub_author", "NA"),
                            ("pub_date", "NA"),
                            ("is_na", True),
                        ]
                    )
                ]
            else:
                print(resource, data_type, dataset, phenocode)
                raise DataException(
                    "No trait found in metadata db for resource {} phenocode: {}".format(
                        resource, phenocode
                    )
                )
        else:
            rows[0]["is_na"] = False
        return rows[0]
