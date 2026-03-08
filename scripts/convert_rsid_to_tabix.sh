#!/bin/bash
# Convert gnomAD bgz file to a csi-indexed rsid lookup TSV.
#
# Uses a fake chromosome "rs" with the numeric part of rsid as position,
# so tabix can query by rsid number (e.g. tabix gs://path/file.tsv.gz rs:12345-12345).
#
# Creates a .csi index because .tbi doesn't allow large enough "coordinates"
#
# Usage: ./convert_rsid_to_tabix.sh <input_bgz> <output_tsv>

set -euo pipefail

if [ $# -ne 2 ]; then
    echo "Usage: $0 <input_bgz> <output_tsv>"
    exit 1
fi

INPUT="$1"
OUTPUT="$2"

echo "Extracting rsids, splitting multi-rsid rows, sorting and compressing..."

(
    echo -e "#rs\tid\trsid\tchr\tpos\tref\talt"
    zcat "$INPUT" \
    | awk -F'\t' 'NR > 1 && $5 != "NA" {
        n = split($5, rsids, ",")
        for (i = 1; i <= n; i++) {
            rsid = rsids[i]
            if (substr(rsid, 1, 2) == "rs") {
                rsid_num = substr(rsid, 3) + 0
                sub("X", 23, $1);
                sub("Y", 24, $1);
                print "rs\t" rsid_num "\t" rsid "\t" $1 "\t" $2 "\t" $3 "\t" $4
            }
        }
    }' \
    | sort -u -t$'\t' -k2,2n --parallel=2 -S 30% -T /mnt/disks/data
) | bgzip -@2 > "${OUTPUT}.gz"

echo "Indexing with tabix (csi index for large \"coordinates\")..."
tabix -C -s 1 -b 2 -e 2 "${OUTPUT}.gz"

echo "Done: ${OUTPUT}.gz and ${OUTPUT}.gz.csi"
