#!/bin/bash
set -euo pipefail
python3 $PWD/log_query.py generate "$@"
