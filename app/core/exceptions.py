class ParseException(Exception):
    pass


class NotFoundException(Exception):
    pass


class DataException(Exception):
    pass


class VariantNotFoundException(NotFoundException):
    pass


class GeneNotFoundException(NotFoundException):
    pass


class PhenoNotFoundException(NotFoundException):
    pass


class ACZeroException(Exception):
    pass
