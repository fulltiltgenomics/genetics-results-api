#!/bin/bash
# Test runner script for genetics-results-api
# This script runs the pytest test suite with various options

set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default server URL
SERVER_URL="${SERVER_URL:-http://localhost:8081}"

# Determine the test directory path
# This script can be run from project root or from tests/ directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
if [[ "$SCRIPT_DIR" == */tests ]]; then
    # Running from tests directory, go up one level
    cd "$SCRIPT_DIR/.."
    TEST_PATH="tests/"
else
    # Running from project root
    TEST_PATH="tests/"
fi

# Parse command line arguments
MODE="${1:-parallel}"

echo -e "${BLUE}=== Genetics Results API Tests ===${NC}"
echo -e "Server URL: ${SERVER_URL}"
echo -e "Working directory: $(pwd)"
echo ""

case "$MODE" in
  parallel|p)
    echo -e "${GREEN}Running tests...${NC}"
    python3 -m pytest ${TEST_PATH} -r w -n auto --server-url "$SERVER_URL" -v
    ;;

  quick|q)
    echo -e "${GREEN}Running quick smoke tests...${NC}"
    python3 -m pytest ${TEST_PATH}test_health.py ${TEST_PATH}test_metadata.py -r w --server-url "$SERVER_URL" -v
    ;;

  credible-sets|cs)
    echo -e "${GREEN}Running credible sets tests...${NC}"
    python3 -m pytest ${TEST_PATH}test_credible_sets.py -r w --server-url "$SERVER_URL" -v
    ;;

  colocalization|coloc)
    echo -e "${GREEN}Running colocalization tests...${NC}"
    python3 -m pytest ${TEST_PATH}test_colocalization.py -r w --server-url "$SERVER_URL" -v
    ;;

  exome-results|exome)
    echo -e "${GREEN}Running exome results tests...${NC}"
    python3 -m pytest ${TEST_PATH}test_exome_results.py -r w --server-url "$SERVER_URL" -v
    ;;

  summary-stats|ss)
    echo -e "${GREEN}Running summary stats tests...${NC}"
    python3 -m pytest ${TEST_PATH}test_summary_stats.py -r w --server-url "$SERVER_URL" -v
    ;;

  collect|c)
    echo -e "${GREEN}Collecting tests (dry run)...${NC}"
    python3 -m pytest ${TEST_PATH} -r w --collect-only
    ;;

  help|h|--help|-h)
    echo "Usage: ./run_tests.sh [MODE]"
    echo ""
    echo "This script can be run from either the project root or the tests/ directory."
    echo ""
    echo "Modes:"
    echo "  parallel (p)         - Run all tests in parallel (default, fast)"
    echo "  sequential (s)       - Run all tests sequentially"
    echo "  quick (q)            - Run only quick smoke tests"
    echo "  credible-sets (cs)   - Run only credible sets tests"
    echo "  colocalization (coloc) - Run only colocalization tests"
    echo "  exome-results (exome) - Run only exome results tests"
    echo "  summary-stats (ss)   - Run only summary stats tests"
    echo "  collect (c)          - Just collect/list tests without running"
    echo "  help (h)             - Show this help message"
    echo ""
    echo "Environment variables:"
    echo "  SERVER_URL           - Server URL to test against (default: http://localhost:8081)"
    echo ""
    echo "Examples:"
    echo "  ./run_tests.sh parallel"
    echo "  ./run_tests.sh quick"
    echo "  SERVER_URL=http://localhost:8081 ./run_tests.sh parallel"
    echo ""
    echo "From project root:"
    echo "  tests/run_tests.sh parallel"
    echo ""
    echo "From tests directory:"
    echo "  cd tests && ./run_tests.sh parallel"
    exit 0
    ;;

  *)
    echo "Unknown mode: $MODE"
    echo "Run './run_tests.sh help' for usage information"
    exit 1
    ;;
esac
