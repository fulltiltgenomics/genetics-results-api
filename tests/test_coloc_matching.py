"""Offline pins for how a variant's credible sets are matched to colocalization pairs.

The credset file and the pair file name a QTL trait differently (feature id against gene
symbol), so a key built from `trait` matched FinnGen sides only and silently left out every
pair the variant reached through its QTL credible set. Rows here are shaped like the served
files; no server and no credentials.
"""

import asyncio

import pytest

from app.config import coloc as coloc_config
from app.config.coloc import coloc_header_schema
from app.core import streams
from app.core.streams import coloc_cs_key, coloc_pair_side_keys, tsv_line_iterator_coloc

HEADER = [
    name.encode()
    for name in coloc_header_schema
    if name not in ("resource1", "version1", "resource2", "version2")
]


def _pair(**cols: str) -> bytes:
    row = {name: b"NA" for name in HEADER}
    row |= {name.encode(): value.encode() for name, value in cols.items()}
    return b"\t".join(row[name] for name in HEADER)


GWAS_SIDE = dict(
    dataset1="FinnGen_R14",
    trait1="RX_STATIN",
    trait1_original="RX_STATIN",
    cell_type1="NA",
    cs1_id="chr19:37264258-46314585_1",
)
# as served: trait2 is the gene symbol, trait2_original the feature id with its suffix
PQTL_PAIR = _pair(
    **GWAS_SIDE,
    dataset2="QTD000584",
    trait2="APOE",
    trait2_original="APOE.5312.49.3..1|aptamer",
    cell_type2="plasma|naive",
    cs2_id="chr19:43905791-45905791_APOE.5312.49.3..1_L2",
)
# the credset file's row for that same credible set
PQTL_CS = coloc_cs_key(
    b"QTD000584",
    b"APOE.5312.49.3..1|aptamer",
    b"plasma|naive",
    b"chr19:43905791-45905791_APOE.5312.49.3..1_L2",
)


class _Registry:
    def get_resource_and_version_bytes_by_dataset(self, dataset: bytes):
        return {b"FinnGen_kanta": (b"finngen", b"R14")}.get(dataset, (b"other", b"NA"))


@pytest.fixture(autouse=True)
def registry(monkeypatch):
    monkeypatch.setattr(streams, "get_dataset_mapping", lambda: _Registry())


def _run(lines: list[bytes], keys, label_versions=None) -> list[list[bytes]]:
    async def stream():
        yield b"\n".join(lines) + b"\n"

    async def collect():
        return [
            row
            async for row in tsv_line_iterator_coloc(
                stream(), HEADER, coloc_pair_side_keys(keys), label_versions
            )
        ]

    return asyncio.run(collect())


def test_pair_is_found_through_the_qtl_side():
    assert len(_run([PQTL_PAIR], {PQTL_CS})) == 1


def test_quantification_suffix_on_one_side_only_still_matches():
    bare = _pair(
        **GWAS_SIDE,
        dataset2="FinnLiver",
        trait2="SREBF2",
        trait2_original="ENSG00000198911",
        cell_type2="liver",
        cs2_id="chr22:1-2_1",
    )
    key = coloc_cs_key(b"FinnLiver", b"ENSG00000198911|ge", b"liver", b"chr22:1-2_1")
    assert len(_run([bare], {key})) == 1


def test_quantifications_of_one_gene_stay_apart_where_the_pair_file_names_them():
    def pair(quant: str) -> bytes:
        return _pair(
            **GWAS_SIDE,
            dataset2="QTD000001",
            trait2="SREBF2",
            trait2_original=f"ENSG00000198911|{quant}",
            cell_type2="liver",
            cs2_id="chr22:1-2_1",
        )

    key = coloc_cs_key(b"QTD000001", b"ENSG00000198911|ge", b"liver", b"chr22:1-2_1")
    rows = _run([pair("ge"), pair("exon")], {key})
    assert [r[4 + HEADER.index(b"trait2_original")] for r in rows] == [
        b"ENSG00000198911|ge"
    ]


def test_same_cs_id_in_another_cell_type_or_trait_does_not_match():
    def pair(trait_original: str, cell_type: str) -> bytes:
        return _pair(
            **GWAS_SIDE,
            dataset2="FinnGen_ATACseq",
            trait2="peak",
            trait2_original=trait_original,
            cell_type2=cell_type,
            cs2_id="chr1:1-2_1",
        )

    key = coloc_cs_key(b"FinnGen_ATACseq", b"peak_a", b"l1.Mono", b"chr1:1-2_1")
    rows = _run(
        [pair("peak_a", "l1.Mono"), pair("peak_a", "l1.B"), pair("peak_b", "l1.Mono")],
        {key},
    )
    assert [r[4 + HEADER.index(b"cell_type2")] for r in rows] == [b"l1.Mono"]
    assert [r[4 + HEADER.index(b"trait2_original")] for r in rows] == [b"peak_a"]


def test_version_comes_from_the_coloc_file_before_the_registry():
    kanta = _pair(
        dataset1="FinnGen_kanta",
        trait1="3000963",
        trait1_original="3000963",
        cell_type1="NA",
        cs1_id="chr1:1-2_1",
        dataset2="FinnGen_ATACseq",
        trait2_original="peak_a",
        cell_type2="l1.Mono",
        cs2_id="chr1:1-2_1",
    )
    key = coloc_cs_key(b"FinnGen_kanta", b"3000963", b"NA", b"chr1:1-2_1")
    assert _run([kanta], {key})[0][:2] == [b"finngen", b"R14"]
    r12 = {b"FinnGen_kanta": (b"finngen", b"R12")}
    assert _run([kanta], {key}, r12)[0][:4] == [b"finngen", b"R12", b"other", b"NA"]


def test_each_profile_coloc_file_attributes_a_shared_label_to_its_own_release():
    """Wherever two paired datasets share a label, each file must resolve it to one of them."""
    from app.config.datasets import datasets

    shared: dict[str, set[str]] = {}
    for entry in coloc_config.coloc:
        for dataset_id in {d for pair in entry["pairs"] for d in pair}:
            labels = datasets[dataset_id].get("dataset") or []
            for label in labels if isinstance(labels, list) else [labels]:
                shared.setdefault(label, set()).add(str(datasets[dataset_id].get("version")))
    ambiguous = {label for label, versions in shared.items() if len(versions) > 1}
    assert ambiguous, "no label is shared between releases any more; this test and the per-file lookup can go"
    for label in ambiguous:
        seen = {
            coloc_config.coloc_label_versions[entry["name"]][label.encode()][1]
            for entry in coloc_config.coloc
            if label.encode() in coloc_config.coloc_label_versions[entry["name"]]
        }
        assert seen == {v.encode() for v in shared[label]}


def test_a_file_pairing_two_releases_under_one_label_is_refused():
    kanta_ids = [
        dataset_id
        for entry in coloc_config.coloc
        for pair in entry["pairs"]
        for dataset_id in pair
        if "kanta" in dataset_id
    ]
    current, old = sorted(set(kanta_ids))[:2]
    with pytest.raises(ValueError, match="cannot be attributed"):
        coloc_config._label_versions({"name": "mixed", "pairs": [(current, old)]})


@pytest.mark.parametrize(
    "cs_id, region",
    [
        ("chr19:37264258-46314585_1", ("19", "37264258", "46314585")),
        # eQTL Catalogue ids carry the molecular trait and a signal label, not a number
        ("chr19:43905791-45905791_APOE.5312.49.3..1_L2", ("19", "43905791", "45905791")),
        ("chrX:100-200_1", ("X", "100", "200")),
    ],
)
def test_credible_set_id_region_is_read_whatever_follows_it(cs_id, region):
    from app.services.data_access_coloc import _CS_ID_REGION

    assert _CS_ID_REGION.match(cs_id).group("chr", "start", "end") == region


@pytest.mark.parametrize("cs_id", ["chr19:1-2", "chr19:1-2_", "19:1-2_1", "chr19:a-2_1", "x"])
def test_malformed_credible_set_id_is_refused(cs_id):
    from app.services.data_access_coloc import _CS_ID_REGION

    assert _CS_ID_REGION.match(cs_id) is None
