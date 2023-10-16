#!/bin/bash

set -e
set -u

isort --check app/ tests/
black --check app/ tests/
flake8 --max-line-length 88 app/

pytest \
    --cov-report xml:test-reports/coverage.xml \
    --cov-report term-missing \
    --cov=app \
    --junitxml=test-reports/junit.xml \
    tests
