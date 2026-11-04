#!/bin/sh
set -eu

# Build stage only, on BUILDPLATFORM: Python 3.10+, the pinned Java 17 JDK, and a locked Maven
# cache at /root/.m2. Coherent official Spark 4.2.1 core/network baselines must exist.
# The Python driver verifies and installs Maven 3.9.15 in its build-only tree.
helper_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
exec python3 "$helper_dir/build_core.py" "$@"
