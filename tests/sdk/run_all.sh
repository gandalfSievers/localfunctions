#!/usr/bin/env bash
# Run all SDK compatibility tests.
# Requires localfunctions to be running with test functions configured.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "=== Python boto3 tests ==="
python -m pytest test_boto3.py -v

echo ""
echo "=== Node.js aws-sdk v3 tests ==="
npm test
