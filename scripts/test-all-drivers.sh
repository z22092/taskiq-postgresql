#!/bin/bash

# Test script for all PostgreSQL drivers
# This script runs tests against all supported drivers

set -e

DRIVERS=("asyncpg" "psycopg" "psqlpy")
PYTHON_VERSIONS=("3.9" "3.10" "3.11" "3.12" "3.13")

echo "🧪 Testing taskiq-postgresql with all drivers"
echo "=============================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    local status=$1
    local message=$2
    case $status in
        "success")
            echo -e "${GREEN}✅ $message${NC}"
            ;;
        "error")
            echo -e "${RED}❌ $message${NC}"
            ;;
        "info")
            echo -e "${YELLOW}ℹ️  $message${NC}"
            ;;
    esac
}

# Check if PostgreSQL is running
check_postgres() {
    print_status "info" "Checking PostgreSQL connection..."
    if ! pg_isready -h localhost -p 5432 -U postgres &>/dev/null; then
        print_status "error" "PostgreSQL is not running or not accessible"
        print_status "info" "Start PostgreSQL with: docker-compose up -d"
        exit 1
    fi
    print_status "success" "PostgreSQL is running"
}

# Test with specific driver
test_driver() {
    local driver=$1
    local python_version=${2:-"3.12"}
    
    print_status "info" "Testing with $driver driver (Python $python_version)"
    
    # Install driver dependencies
    if ! uv sync --group "$driver" &>/dev/null; then
        print_status "error" "Failed to install $driver dependencies"
        return 1
    fi
    
    # Set environment variables
    export TEST_DRIVER="$driver"
    export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/postgres"
    
    # Run tests
    if uv run pytest tests/ -v --tb=short -x; then
        print_status "success" "$driver tests passed"
        return 0
    else
        print_status "error" "$driver tests failed"
        return 1
    fi
}

# Test all drivers
test_all_drivers() {
    local failed_drivers=()
    
    for driver in "${DRIVERS[@]}"; do
        echo
        echo "----------------------------------------"
        if test_driver "$driver"; then
            print_status "success" "$driver: ALL TESTS PASSED"
        else
            print_status "error" "$driver: TESTS FAILED"
            failed_drivers+=("$driver")
        fi
    done
    
    echo
    echo "=========================================="
    echo "📊 Test Summary"
    echo "=========================================="
    
    for driver in "${DRIVERS[@]}"; do
        if [[ " ${failed_drivers[*]} " =~ " ${driver} " ]]; then
            print_status "error" "$driver: FAILED"
        else
            print_status "success" "$driver: PASSED"
        fi
    done
    
    if [ ${#failed_drivers[@]} -eq 0 ]; then
        print_status "success" "All drivers passed tests! 🎉"
        return 0
    else
        print_status "error" "Some drivers failed tests"
        return 1
    fi
}

# Test with specific Python version
test_with_python() {
    local python_version=$1
    print_status "info" "Testing with Python $python_version"
    
    if ! uv python install "$python_version" &>/dev/null; then
        print_status "error" "Failed to install Python $python_version"
        return 1
    fi
    
    # Set Python version for UV
    uv python pin "$python_version"
    
    test_all_drivers
}

# Performance test
run_performance_tests() {
    print_status "info" "Running performance tests..."
    
    export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/postgres"
    
    if uv run pytest tests/ -v -m "performance" --tb=short; then
        print_status "success" "Performance tests passed"
    else
        print_status "error" "Performance tests failed"
    fi
}

# Integration test
run_integration_tests() {
    print_status "info" "Running integration tests..."
    
    export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/postgres"
    
    if uv run pytest tests/ -v -m "integration" --tb=short; then
        print_status "success" "Integration tests passed"
    else
        print_status "error" "Integration tests failed"
    fi
}

# Main script logic
main() {
    case "${1:-all}" in
        "all")
            check_postgres
            test_all_drivers
            ;;
        "driver")
            if [ -z "$2" ]; then
                echo "Usage: $0 driver <driver_name>"
                echo "Available drivers: ${DRIVERS[*]}"
                exit 1
            fi
            check_postgres
            test_driver "$2"
            ;;
        "python")
            if [ -z "$2" ]; then
                echo "Usage: $0 python <python_version>"
                echo "Available versions: ${PYTHON_VERSIONS[*]}"
                exit 1
            fi
            check_postgres
            test_with_python "$2"
            ;;
        "performance")
            check_postgres
            run_performance_tests
            ;;
        "integration")
            check_postgres
            run_integration_tests
            ;;
        "help"|"--help"|"-h")
            echo "Usage: $0 [command] [options]"
            echo
            echo "Commands:"
            echo "  all                    Test all drivers (default)"
            echo "  driver <name>          Test specific driver"
            echo "  python <version>       Test with specific Python version"
            echo "  performance           Run performance tests"
            echo "  integration           Run integration tests"
            echo "  help                  Show this help"
            echo
            echo "Available drivers: ${DRIVERS[*]}"
            echo "Available Python versions: ${PYTHON_VERSIONS[*]}"
            exit 0
            ;;
        *)
            print_status "error" "Unknown command: $1"
            echo "Use '$0 help' for usage information"
            exit 1
            ;;
    esac
}

# Run main function
main "$@"