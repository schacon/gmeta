#!/bin/bash
set -eu -o pipefail

git init
echo "initial content" > README
git add .
git commit -m "initial"
