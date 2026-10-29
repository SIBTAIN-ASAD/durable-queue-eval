#!/bin/bash
set -euo pipefail
mkdir -p /logs/verifier
printf '0\n' > /logs/verifier/reward.txt
chmod -R go-rwx /tests /logs/verifier
cd /tests
if /usr/local/bin/python3 -I /tests/verify.py; then
    printf '1\n' > /logs/verifier/reward.txt
fi
