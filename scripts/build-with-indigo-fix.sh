#!/bin/sh
set -eu

# Build bootes against the bundled indigo streaming-CAR fix. The patch lives in
# a git bundle because the current maintainer identity cannot push a branch to
# bluesky-social/indigo. Override INDIGO_DIR or VALSGO_DIR when needed.
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
INDIGO_DIR=${INDIGO_DIR:-"$ROOT/.indigo-stream-fix"}
VALSGO_DIR=${VALSGO_DIR:-"$ROOT/../vals-examples/valsgo"}
OUTPUT=${OUTPUT:-"$ROOT/bootes"}
BASE=8b43a326dbbb394f63b6d68761553cdfe25532de

if [ ! -d "$INDIGO_DIR/.git" ]; then
  git clone https://github.com/bluesky-social/indigo.git "$INDIGO_DIR"
fi
git -C "$INDIGO_DIR" fetch origin "$BASE"
git -C "$INDIGO_DIR" fetch "$ROOT/indigo-stream-fix.bundle" vals-stream-fix
git -C "$INDIGO_DIR" checkout --detach --force FETCH_HEAD

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT HUP INT TERM
(
  cd "$WORK"
  go work init "$ROOT" "$INDIGO_DIR" "$VALSGO_DIR"
)
GOWORK="$WORK/go.work" go test "$ROOT/..."
GOWORK="$WORK/go.work" go build -o "$OUTPUT" "$ROOT"
