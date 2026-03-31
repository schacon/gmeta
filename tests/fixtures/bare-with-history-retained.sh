#!/bin/bash
# Creates a bare repo with 2 commits on refs/meta/main:
#   Commit 1 (older): project/old_key/__value = "old_value"
#   Commit 2 (tip):   project/old_key/__value = "old_value"
#                      project/testing/__value = "hello"
#
# Unlike bare-with-history.sh, the tip commit retains old_key in its tree.
# Tip commit message only mentions 'testing' (old_key was added in commit 1).
set -eu -o pipefail

git init --bare

# --- Commit 1: project/old_key/__value = "old_value" ---
BLOB1=$(echo -n '"old_value"' | git hash-object -w --stdin)
VAL_TREE1=$(printf '100644 blob %s\t__value\n' "$BLOB1" | git mktree)
PROJ_TREE1=$(printf '040000 tree %s\told_key\n' "$VAL_TREE1" | git mktree)
ROOT_TREE1=$(printf '040000 tree %s\tproject\n' "$PROJ_TREE1" | git mktree)

COMMIT1=$(printf 'gmeta: serialize (1 changes)\n\nA\tproject\told_key' \
    | git commit-tree "$ROOT_TREE1")

# --- Commit 2 (tip): both old_key and testing ---
BLOB2=$(echo -n '"hello"' | git hash-object -w --stdin)
VAL_TREE2=$(printf '100644 blob %s\t__value\n' "$BLOB2" | git mktree)
# Build project tree with both entries
PROJ_TREE2=$(printf '040000 tree %s\told_key\n040000 tree %s\ttesting\n' \
    "$VAL_TREE1" "$VAL_TREE2" | git mktree)
ROOT_TREE2=$(printf '040000 tree %s\tproject\n' "$PROJ_TREE2" | git mktree)

COMMIT2=$(printf 'gmeta: serialize (1 changes)\n\nA\tproject\ttesting' \
    | git commit-tree "$ROOT_TREE2" -p "$COMMIT1")

git update-ref "refs/meta/main" "$COMMIT2"
