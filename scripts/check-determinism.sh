#!/usr/bin/env bash
# Runs the same seeds in the JVM and in the browser build (with Node) and checks they agree exactly:
# same elections, messages, commits, and the same client history, down to its hash.
set -euo pipefail
cd "$(dirname "$0")/.."
SEEDS=${1:-20}
java -jar ordito-sim/target/ordito-sim.jar --seed 1 --runs "$SEEDS" --fingerprint > /tmp/ordito-jvm.txt
cp ordito-playground/target/site/ordito.js /tmp/ordito-playground.cjs
node -e '
  const m = require("/tmp/ordito-playground.cjs");
  for (let s = 1; s <= Number(process.argv[1]); s++) console.log(m.orditoFingerprint(s, 5));
' "$SEEDS" > /tmp/ordito-js.txt
if diff /tmp/ordito-jvm.txt /tmp/ordito-js.txt; then
  echo "JVM and browser agree on $SEEDS seeds:"
  head -3 /tmp/ordito-jvm.txt
else
  echo "JVM and browser runs differ" >&2
  exit 1
fi
