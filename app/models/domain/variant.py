import re
from app.core.exceptions import ParseException

var_re = re.compile("-|_|:|\\|")
non_autosomes = set(["X", "Y", "XY", "MT"])


class Variant(object):
    def __init__(self, varstr: str) -> None:
        s = var_re.split(varstr)
        if len(s) != 4:
            raise ParseException(
                "variant needs to contain four fields, supported separators are - _ : |"
            )
        try:
            chr = re.sub(r"^0", "", str(s[0]))
            chr = (
                chr.upper()
                .replace("CHR", "")
                .replace("23", "X")
                .replace("24", "Y")
                .replace("25", "XY")
                .replace("26", "MT")
            )
            chr_int = int(chr)
            if chr_int < 1 or chr_int > 26:
                raise ValueError
        except ValueError:
            if chr not in non_autosomes:
                raise ParseException("supported chromosomes: 1-26,X,Y,XY,MT")
        try:
            pos = int(s[1])
        except ValueError:
            raise ParseException("position must be an integer")
        self.chr = chr
        self.pos = pos
        self.ref = s[2].upper()
        self.alt = s[3].upper()
        if not bool(re.match(r"[ACGT]+$", self.ref)) or not bool(
            re.match(r"[ACGT]+$", self.alt)
        ):
            raise ParseException("only ACGT alleles are supported")
        self.varid = "{}-{}-{}-{}".format(self.chr, self.pos, self.ref, self.alt)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Variant):
            return NotImplemented
        return (
            self.chr == other.chr
            and self.pos == other.pos
            and self.ref == other.ref
            and self.alt == other.alt
        )

    def __hash__(self) -> int:
        return hash(self.varid)

    def __repr__(self) -> str:
        return self.varid

    def ot_repr(self) -> str:
        return "{}_{}_{}_{}".format(self.chr, self.pos, self.ref, self.alt)
