from typing_extensions import Literal, NotRequired, TypedDict


Datatype = Literal["GWAS", "eQTL", "sQTL", "pQTL", "edQTL", "metaboQTL", "NA"]
TraitType = Literal["case-control", "continuous"]


class AssocPheno(TypedDict):
    category: str
    resource: str
    num_cases: NotRequired[int]
    num_controls: NotRequired[int]
    num_samples: NotRequired[int]
    phenocode: str
    phenostring: str
    trait_type: TraitType
    pub_author: NotRequired[str]
    pub_date: NotRequired[str]
    is_na: NotRequired[bool]


class Results(TypedDict):
    variant: str
    time: float


class FineMappedResult(TypedDict):
    resource: str
    dataset: str
    data_type: Datatype
    phenocode: str
    mlog10p: float
    beta: float
    se: float
    cs_size: int
    cs_min_r2: float
    pip: float


class FineMappedResultContainer(TypedDict):
    data: list[FineMappedResult]
    resources: list[str]


class FineMappedResults(Results):
    finemapped: FineMappedResultContainer


class AssociationResult(TypedDict):
    resource: str
    dataset: str
    data_type: Datatype
    phenocode: str
    mlogp: float
    beta: float
    sebeta: float


class AssociationResultContainer(TypedDict):
    data: list[AssociationResult]
    resources: list[str]


class AssociationResults(Results):
    assoc: AssociationResultContainer


class ResponseTime(TypedDict):
    gnomad: float
    finemapped: float
    assoc: float
    total: float
