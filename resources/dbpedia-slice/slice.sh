#!/usr/bin/env bash
# Cut an entity-closed slice out of a DBpedia N-Triples dump.
#
# Seeds are sampled per group from rdf:type triples, expanded along resource
# edges for --hops rounds, and every triple whose subject is in the resulting
# entity set is kept. Each round is one streaming pass over the input.
set -euo pipefail

usage() {
    cat >&2 <<'EOF'
Usage: slice.sh -i INPUT -o OUTPUT_DIR [-s SEED] [-H HOPS] [-L LANG] [-p PARENT_ID] [-l LICENSE] GROUP...

  -i INPUT       N-Triples dump (.nt, .nt.bz2)
  -o OUTPUT_DIR  dataset directory, must not exist; its name is the dataset id
  -s SEED        sampling seed (default 42)
  -H HOPS        expansion rounds along dbr: object edges (default 1)
  -L LANG        drop literals tagged with another language (untagged and
                 typed literals are kept); default: keep all
  -p PARENT_ID   dataset id of INPUT (default: id in INPUT's .odibel/dataset.yaml,
                 otherwise the input file name)
  -l LICENSE     SPDX id of the output (default CC-BY-SA-3.0, DBpedia)
  GROUP          N:CLASS[,CLASS...]  sample N subjects (or "all") typed with
                 any of the classes; classes as dbo:Name or full IRI

Example:
  slice.sh -i data/dbpedia-2022.12.01-selected/selected.nt.bz2 \
    -o data/dbpedia-2022.12.01-mini-food-s42 -s 42 500:dbo:Food
EOF
    exit 1
}

input="" out="" seed=42 hops=1 lang="" parent_id="" license="CC-BY-SA-3.0"
while getopts "i:o:s:H:L:p:l:" opt; do
    case "$opt" in
        i) input="$OPTARG" ;;
        o) out="$OPTARG" ;;
        s) seed="$OPTARG" ;;
        H) hops="$OPTARG" ;;
        L) lang="$OPTARG" ;;
        p) parent_id="$OPTARG" ;;
        l) license="$OPTARG" ;;
        *) usage ;;
    esac
done
shift $((OPTIND - 1))
[[ -n "$input" && -n "$out" && $# -gt 0 ]] || usage
[[ -f "$input" ]] || { echo "input not found: $input" >&2; exit 1; }
[[ ! -e "$out" ]] || { echo "output exists: $out" >&2; exit 1; }

export LC_ALL=C
AWK=$(command -v mawk || command -v awk)
case "$input" in
    *.bz2) CAT=$(command -v lbzcat || command -v bzcat) ;;
    *.gz) CAT="zcat" ;;
    *) CAT="cat" ;;
esac
ZIP=$(command -v lbzip2 || command -v bzip2)
RDF_TYPE="<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
DBR="<http://dbpedia.org/resource/"

expand_class() {
    case "$1" in
        dbo:*) echo "<http://dbpedia.org/ontology/${1#dbo:}>" ;;
        \<*\>) echo "$1" ;;
        *) echo "<$1>" ;;
    esac
}

# Deterministic shuf: GNU coreutils' documented seeded random source.
seeded_random() {
    openssl enc -aes-256-ctr -pass pass:"$1" -nosalt </dev/zero 2>/dev/null
}

mkdir -p "$out/seeds"
start=$(date -u +%Y-%m-%dT%H:%M:%SZ)

# group index <TAB> class IRI
classmap="$out/seeds/classes.tsv"
: >"$classmap"
counts=()
for i in $(seq 0 $(($# - 1))); do
    spec="${@:$((i + 1)):1}"
    n="${spec%%:*}"
    classes="${spec#*:}"
    [[ "$n" == "all" || "$n" =~ ^[0-9]+$ ]] || { echo "bad group: $spec" >&2; exit 1; }
    counts+=("$n")
    IFS=',' read -ra cls <<<"$classes"
    for c in "${cls[@]}"; do
        printf '%s\t%s\n' "$i" "$(expand_class "$c")" >>"$classmap"
    done
done

echo "[1] collecting typed subjects" >&2
$CAT "$input" | $AWK -v T="$RDF_TYPE" -v dir="$out/seeds" '
    NR == FNR { split($0, f, "\t"); g[f[2]] = g[f[2]] " " f[1]; next }
    $2 == T && ($3 in g) {
        n = split(g[$3], gs, " ")
        for (k = 1; k <= n; k++) print $1 > (dir "/candidates_" gs[k] ".txt")
    }
' "$classmap" -

: >"$out/seeds/seeds.txt"
for i in "${!counts[@]}"; do
    cand="$out/seeds/candidates_$i.txt"
    touch "$cand"
    sort -u -o "$cand" "$cand"
    if [[ "${counts[$i]}" == "all" ]]; then
        cp "$cand" "$out/seeds/group_$i.txt"
    else
        shuf -n "${counts[$i]}" --random-source=<(seeded_random "$seed-$i") "$cand" \
            | sort >"$out/seeds/group_$i.txt"
    fi
    echo "    group $i: $(wc -l <"$cand") candidates, $(wc -l <"$out/seeds/group_$i.txt") seeds" >&2
    cat "$out/seeds/group_$i.txt" >>"$out/seeds/seeds.txt"
done
sort -u -o "$out/seeds/seeds.txt" "$out/seeds/seeds.txt"
[[ -s "$out/seeds/seeds.txt" ]] || { echo "no seeds found" >&2; exit 1; }

cp "$out/seeds/seeds.txt" "$out/entities.txt"
for h in $(seq 1 "$hops"); do
    echo "[2] expansion round $h" >&2
    $CAT "$input" | $AWK -v D="$DBR" '
        NR == FNR { e[$1]; next }
        ($1 in e) && index($3, D) == 1 { print $3 }
    ' "$out/entities.txt" - | cat "$out/entities.txt" - | sort -u >"$out/entities.next"
    mv "$out/entities.next" "$out/entities.txt"
    echo "    entities: $(wc -l <"$out/entities.txt")" >&2
done

echo "[3] writing slice" >&2
$CAT "$input" | $AWK -v L="$lang" '
    NR == FNR { e[$1]; next }
    ($1 in e) {
        if (L != "" && match($0, /"@[A-Za-z0-9-]+[ \t]*\.[ \t]*$/)) {
            tag = substr($0, RSTART + 2)
            sub(/[ \t]*\.[ \t]*$/, "", tag)
            if (tag != L) next
        }
        print
    }
' "$out/entities.txt" - | sort -u | $ZIP >"$out/data.nt.bz2"

triples=$($CAT "$out/data.nt.bz2" | wc -l)
echo "    triples: $triples" >&2

echo "[4] writing .odibel metadata" >&2
input_dir=$(dirname "$input")
if [[ -z "$parent_id" && -f "$input_dir/.odibel/dataset.yaml" ]]; then
    parent_id=$(sed -n 's/^id: *"\{0,1\}\([^"]*\)"\{0,1\} *$/\1/p' "$input_dir/.odibel/dataset.yaml" | head -1)
fi
[[ -n "$parent_id" ]] || parent_id=$(basename "$input")

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
git_commit=$(git -C "$script_dir" rev-parse HEAD 2>/dev/null || echo unknown)
git_dirty=false
[[ -z "$(git -C "$script_dir" status --porcelain -- . 2>/dev/null)" ]] || git_dirty=true

yaml_list() {
    for x in "$@"; do printf '    - "%s"\n' "$x"; done
}

# Top-N "count value" lines as a JSON object.
top_json() {
    sort | uniq -c | sort -k1,1nr -k2 | head -n "$1" | $AWK '
        BEGIN { printf "{" }
        { v = $2; gsub(/\\/, "\\\\", v); gsub(/"/, "\\\"", v); printf "%s\"%s\": %d", (NR > 1 ? ", " : ""), v, $1 }
        END { printf "}" }'
}

mkdir -p "$out/.odibel"
data_nt="$out/data.nt.bz2"
cat >"$out/.odibel/stats.json" <<EOF
{
  "triples": $triples,
  "subjects": $($CAT "$data_nt" | cut -d' ' -f1 | uniq | wc -l),
  "predicates": $($CAT "$data_nt" | cut -d' ' -f2 | sort -u | wc -l),
  "seeds": $(wc -l <"$out/seeds/seeds.txt"),
  "entities": $(wc -l <"$out/entities.txt"),
  "top_predicates": $($CAT "$data_nt" | cut -d' ' -f2 | top_json 25),
  "top_types": $($CAT "$data_nt" | $AWK -v T="$RDF_TYPE" '$2 == T { print $3 }' | top_json 25),
  "literal_languages": $($CAT "$data_nt" | grep -o '"@[A-Za-z-]*[[:space:]]*\.$' | sed 's/^"@\([A-Za-z-]*\).*/\1/' | top_json 25)
}
EOF

cat >"$out/.odibel/dataset.yaml" <<EOF
odibel: "0.1"
id: "$(basename "$out")"
title: "DBpedia slice of $parent_id ($*)"
status: draft
kind: rdf-ntriples
license: "$license"
created_at: "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
derived_from:
  - id: "$parent_id"
    file: "$(basename "$input")"
    sha256: "$(sha256sum "$input" | cut -d' ' -f1)"
recipe:
  script: resources/dbpedia-slice/slice.sh
  git_commit: "$git_commit"
  git_dirty: $git_dirty
  seed: "$seed"
  hops: $hops
  literal_language: "${lang:-all}"
  groups:
$(yaml_list "$@")
  started_at: "$start"
  tools:
$(yaml_list "$($AWK -W version 2>&1 | head -1)" "$(shuf --version | head -1)" "$(openssl version)")
files:
  data.nt.bz2: sorted, distinct N-Triples
  entities.txt: final entity set
  seeds/: sampled seeds per group (group_N.txt), union (seeds.txt), candidates per group
contract:
  format: n-triples
  compression: bzip2
  closure: every triple whose subject is in entities.txt is present (as in the parent)$([[ -z "$lang" ]] || printf ', except literals tagged with a language other than %s' "$lang")
  type_triples: as in the parent
  literal_languages: "$([[ -n "$lang" ]] && echo "$lang and untagged" || echo "as in the parent")"
EOF

(cd "$out" && find . -path ./.odibel -prune -o -type f -print | sed 's|^\./||' | sort \
    | while read -r f; do sha256sum "$f"; done >.odibel/files.sha256)
echo "done: $out" >&2
