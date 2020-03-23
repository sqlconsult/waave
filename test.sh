#!/usr/bin/env bash

clear
# -s = show output, do not capture
# -v = verbose
pytest -s -v test_api_calls.py
pytest -s -v test_db.py
rm -rf .pytest_cache