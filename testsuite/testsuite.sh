#!/bin/bash

# Automatically set the working directory to the script's location
cd "$(dirname "$0")" || exit 1

WIKIURL="../wikiurl"
WORKDIR="."

echo "========================================"
echo " Starting wikiurl comprehensive test suite"
echo "========================================"

# Clean up previous test artifacts
rm -f *.tsv *.jsonl *.raw *.articles *.gz *_temp*
echo "[Setup] Cleared old test files."

# Helper function to run the engine and calculate execution time
run_test() {
    local eng=$1
    local cmd=$2
    
    local start_time=$SECONDS
    
    # Run the command silently (errors will still print to stderr)
    eval "$WIKIURL $cmd"
    
    local elapsed=$(( SECONDS - start_time ))
    local mins=$(( elapsed / 60 ))
    local secs=$(( elapsed % 60 ))
    
    # Print formatted output: -> Engine: api      (0:05)
    printf "  -> Engine: %-8s (%d:%02d)\n" "$eng" "$mins" "$secs"
}

# ---------------------------------------------------------
# Test Group 1: REGEX Filtering (Supported by all 4 engines)
# Domain: cnn.com
# ---------------------------------------------------------
echo -e "\n---> Running Group 1: Regex Filtering (cnn.com)"
declare -a engines_all=("api" "sql" "stream" "download")

for eng in "${engines_all[@]}"; do
    cmd="-d cnn.com -s simplewiki -m $eng -r 'edition.cnn.com/2006' --genTsv --genJson -w $WORKDIR"
    run_test "$eng" "$cmd"
done

# ---------------------------------------------------------
# Test Group 2: NAMESPACE Filtering (API and SQL only)
# Domain: pbs.org 
# ---------------------------------------------------------
echo -e "\n---> Running Group 2: Namespace Filtering (pbs.org)"
declare -a engines_ns=("api" "sql")

for eng in "${engines_ns[@]}"; do
    cmd="-d pbs.org -s simplewiki -m $eng -n 0,2 --genTsv --genJson -w $WORKDIR"
    run_test "$eng" "$cmd"
done

# ---------------------------------------------------------
# Test Group 3: FULL DUMP (Baseline TSV/JSON extraction)
# Domain: nasa.gov
# ---------------------------------------------------------
echo -e "\n---> Running Group 3: Baseline Extraction (nasa.gov)"
for eng in "${engines_all[@]}"; do
    cmd="-d nasa.gov -s simplewiki -m $eng --genTsv --genJson -w $WORKDIR"
    run_test "$eng" "$cmd"
done

# ---------------------------------------------------------
# Test Group 4: ARTICLES Generation (API only)
# Domain: fcc.gov 
# ---------------------------------------------------------
echo -e "\n---> Running Group 4: Articles Format (fcc.gov)"
cmd="-d fcc.gov -s simplewiki -m api --genArticles -w $WORKDIR"
run_test "api" "$cmd"

# ---------------------------------------------------------
# Test Group 5: RAW Output Generation (API, Stream, Download)
# Domain: who.int
# ---------------------------------------------------------
echo -e "\n---> Running Group 5: Raw Format (who.int)"
declare -a engines_raw=("api" "stream" "download")

for eng in "${engines_raw[@]}"; do
    cmd="-d who.int -s simplewiki -m $eng --genRaw -w $WORKDIR"
    run_test "$eng" "$cmd"
done

echo -e "\n========================================"
echo " Tests finished. Output line counts for comparison:"
echo " (Matching counts indicate engines are in sync)"
echo "========================================"
# The 2>/dev/null hides errors if a file type wasn't generated
wc -l *.tsv *.jsonl *.articles *.raw 2>/dev/null | sort -k2
echo "========================================"
