import re
from app.core.exceptions import ParseException

var_re = re.compile("-|_|:|\\|")


class Variant(object):
    def __init__(self, varstr: str) -> None:
        s = var_re.split(varstr)
        if len(s) != 4:
            raise ParseException(
                "variant needs to contain four fields, supported separators are - _ : |"
            )
        try:
            chr = re.sub(r"^0", "", str(s[0]))
            chr = chr.upper().replace("CHR", "").replace("X", "23")
            chr_int = int(chr)
            if chr_int < 1 or chr_int > 23:
                raise ValueError
        except ValueError:
            raise ParseException("supported chromosomes: 1-23,X")
        try:
            pos = int(s[1])
        except ValueError:
            raise ParseException("position must be an integer")
        self.chr = chr_int
        self.pos = pos
        self.ref = s[2].upper()
        self.alt = s[3].upper()
        self.chr_bytes = str(chr_int).encode()
        self.pos_bytes = str(pos).encode()
        self.ref_bytes = self.ref.encode()
        self.alt_bytes = self.alt.encode()
        if not bool(re.match(r"[ACGT]+$", self.ref)) or not bool(
            re.match(r"[ACGT]+$", self.alt)
        ):
            raise ParseException("only ACGT alleles are supported")
        self.varid = "{}-{}-{}-{}".format(self.chr, self.pos, self.ref, self.alt)

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, Variant):
            return NotImplemented
        return (self.chr, self.pos, self.ref, self.alt) < (
            other.chr,
            other.pos,
            other.ref,
            other.alt,
        )

    def __le__(self, other: object) -> bool:
        if not isinstance(other, Variant):
            return NotImplemented
        return (self.chr, self.pos, self.ref, self.alt) <= (
            other.chr,
            other.pos,
            other.ref,
            other.alt,
        )

    def __gt__(self, other: object) -> bool:
        if not isinstance(other, Variant):
            return NotImplemented
        return (self.chr, self.pos, self.ref, self.alt) > (
            other.chr,
            other.pos,
            other.ref,
            other.alt,
        )

    def __ge__(self, other: object) -> bool:
        if not isinstance(other, Variant):
            return NotImplemented
        return (self.chr, self.pos, self.ref, self.alt) >= (
            other.chr,
            other.pos,
            other.ref,
            other.alt,
        )

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
