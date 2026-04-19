#!/bin/bash
# Creates a bare repo with refs/${NS}/main containing:
#   project/testing/__value = "hello"
#
# Usage: bare-with-meta.sh [namespace]
# Default namespace: meta
set -eu -o pipefail

NS="${1:-meta}"

git init --bare

# Build the tree structure expected by gmeta:
#   project/
#     testing/
#       __value  (blob: "hello")
BLOB=$(echo -n '"hello"' | git hash-object -w --stdin)
VALUE_TREE=$(printf '100644 blob %s\t__value\n' "$BLOB" | git mktree)
PROJECT_TREE=$(printf '040000 tree %s\ttesting\n' "$VALUE_TREE" | git mktree)
ROOT_TREE=$(printf '040000 tree %s\tproject\n' "$PROJECT_TREE" | git mktree)

COMMIT=$(git commit-tree "$ROOT_TREE" -m "initial meta")
git update-ref "refs/$NS/main" "$COMMIT"
