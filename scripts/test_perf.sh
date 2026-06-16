#!/usr/bin/env bash
#
# test_perf.sh — time every results-api endpoint with curl against local dev.
#
# Sends one query per endpoint and reports curl timings. Multi-resource
# fan-out queries are grouped first since they are the slowest.
#
# Usage:
#   uv run python run_server.py 2000      # start local dev server first
#   scripts/test_perf.sh
#
# Env overrides:
#   BASE     API base URL              (default http://localhost:2000/api/v1)
#   REPEAT   runs per query            (default 3)
#   CS_RES / CS_PHENO / CS_ID  credible-set id for the *_by_id endpoints
#                              (auto-discovered from a region query if unset)

set -u
export LC_ALL=C  # ensure '.' decimal separator so `sort -n` orders times correctly

BASE="${BASE:-http://localhost:2000/api/v1}"
REPEAT="${REPEAT:-3}"
ORIGIN="${BASE%/api/v1}"

echo "BASE=$BASE  REPEAT=$REPEAT"

# ---- timing helper -----------------------------------------------------------
# run METHOD PATH [JSON_BODY] — times REPEAT calls, prints each + the min
run() {
  local method="$1" path="$2" body="${3:-}"
  local url="$BASE$path"
  printf '%-5s %s\n' "$method" "$path"
  local times=() out t rest code size min i
  for i in $(seq 1 "$REPEAT"); do
    if [ "$method" = GET ]; then
      out=$(curl -s -o /dev/null -w "%{time_total} %{http_code} %{size_download}" "$url")
    else
      out=$(curl -s -o /dev/null -w "%{time_total} %{http_code} %{size_download}" \
            -H "Content-Type: application/json" -X POST -d "$body" "$url")
    fi
    t=${out%% *}; rest=${out#* }; code=${rest%% *}; size=${rest##* }
    times+=("$t")
    printf '    %8ss  HTTP %s  %sB\n' "$t" "$code" "$size"
  done
  min=$(printf '%s\n' "${times[@]}" | sort -n | head -1)
  printf '    -> min %ss\n' "$min"
}

section() { printf '\n========== %s ==========\n' "$1"; }

# ---- discover a real credible-set id for the *_by_id endpoints ---------------
if [ -z "${CS_ID:-}" ] && command -v python3 >/dev/null 2>&1; then
  disc=$(curl -s "$BASE/credible_sets_by_region/19:44900000-45000000?format=json" \
    | python3 -c '
import sys, json
try:
    data = json.load(sys.stdin)
except Exception:
    sys.exit(0)
if isinstance(data, list) and data:
    r = data[0]
    res = r.get("resource", "")
    ph  = r.get("trait") or r.get("trait_original") or r.get("dataset", "")
    cs  = r.get("cs_id", "")
    if res and ph and cs:
        print(f"{res}\t{ph}\t{cs}")
' 2>/dev/null)
  if [ -n "$disc" ]; then
    CS_RES=$(printf '%s' "$disc" | cut -f1)
    CS_PHENO=$(printf '%s' "$disc" | cut -f2)
    CS_ID=$(printf '%s' "$disc" | cut -f3)
    echo "Discovered cs_id for *_by_id endpoints: $CS_RES / $CS_PHENO / $CS_ID"
  fi
fi

# =============================================================================
section "FAN-OUT (multi-resource queries)"
run GET  "/credible_sets_by_variant/19-44908684-T-C"
run GET  "/credible_sets_by_region/19:44900000-45000000"
run GET  "/credible_sets_by_gene/APOE"
run GET  "/credible_sets_by_qtl_gene/PCSK9"
run POST "/credible_sets_by_variant" '{"variants":"19-44908684-T-C,1-55039974-G-T"}'
run GET  "/colocalization_by_variant/1-55039974-G-T"
run GET  "/expression_by_gene/PCSK9"
run GET  "/exome_results_by_region/1:1000000-2000000"
run GET  "/peak_to_genes/chr1-817095-817594"
run GET  "/summary_stats/finngen/gwas?variants=19-44908684-T-C&phenotypes=I9_HYPERLIPID"

section "Credible sets"
run GET  "/credible_sets_by_phenotype/finngen/AUTOIMMUNE"
run GET  "/credible_sets/finngen_gwas/stats"
if [ -n "${CS_ID:-}" ]; then
  run GET "/credible_sets_by_id/${CS_RES}/${CS_PHENO}/${CS_ID}"
else
  echo "SKIP  /credible_sets_by_id/... (set CS_RES, CS_PHENO, CS_ID — no id auto-discovered)"
fi

section "Colocalization"
run GET  "/colocalization_by_variant/1-55039974-G-T/finngen/I9_HYPERLIPID"
if [ -n "${CS_ID:-}" ]; then
  run GET "/colocalization_by_credible_set_id/${CS_RES}/${CS_PHENO}/${CS_ID}"
else
  echo "SKIP  /colocalization_by_credible_set_id/... (needs CS_RES, CS_PHENO, CS_ID)"
fi

section "Summary stats (POST)"
run POST "/summary_stats/finngen/gwas" '{"variants":["19-44908684-T-C"],"phenotypes":["I9_HYPERLIPID"]}'

section "Variant annotation"
run GET  "/variant_annotation/finngen?variant=19-44908684-T-C"
run GET  "/variant_annotation/gnomad?variant=1-55039974-G-T"
run POST "/variant_annotation/finngen" '{"variants":["19-44908684-T-C","1-55039974-G-T"]}'

section "Exome results"
run GET  "/exome_results_by_phenotype/genebass/categorical_41210_both_sexes_S068_"
run GET  "/exome_results_by_variant/1-925947-C-T"
run GET  "/exome_results_by_gene/SAMD11"
run GET  "/gene_based/SAMD11"

section "Genes / search"
run GET  "/genes_in_region/1/1000000/2000000"
run GET  "/nearest_genes/19-44908684-T-C"
run POST "/nearest_genes" '{"variants":["19-44908684-T-C","1-55039974-G-T"]}'
run GET  "/search?q=PCSK9"
run GET  "/gene_disease/BRCA1"
run GET  "/gene/normalize?symbols=GPT,BRCA2"
run GET  "/gene_group/members?group_name=Frizzled%20receptors"

section "RSID"
run GET  "/rsid/variants?rsids=rs11591147,rs7412"
run POST "/rsid/variants" '{"rsids":["rs11591147","rs7412"]}'

section "Metadata / catalog"
run GET  "/resource_metadata/finngen"
run GET  "/trait_name_mapping"
run GET  "/datasets"
run GET  "/resources"
run GET  "/phenotype/finngen/I9_HYPERLIPID/markdown"

section "Health"
printf '%-5s %s\n' GET "$ORIGIN/healthz"
for i in $(seq 1 "$REPEAT"); do
  printf '    %8ss  HTTP %s\n' \
    $(curl -s -o /dev/null -w "%{time_total} %{http_code}" "$ORIGIN/healthz")
done
