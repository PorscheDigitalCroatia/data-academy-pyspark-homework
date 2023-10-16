#!/bin/bash

set -e
set -u

isort app/ tests/
black app/ tests/
