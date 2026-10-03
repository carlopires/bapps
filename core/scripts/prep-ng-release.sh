#!/usr/bin/env bash
# Prepare a "glommio-ng" stopgap release from the current tree.
#
# Applies packaging metadata only (crate name, lib name, readme path,
# description, repository/homepage), runs cargo package, and restores the
# tree afterwards. Nothing is committed; no code behaviour changes.
#
# Usage:
#   scripts/prep-ng-release.sh            # package + verify build
#   scripts/prep-ng-release.sh --dry-run  # also run cargo publish --dry-run
#   scripts/prep-ng-release.sh --publish  # package, verify, then publish

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

NG_NAME="glommio-ng"
NG_REPO="https://github.com/dahankzter/glommio"
NG_BLURB="Stopgap republish of the community glommio fork (github.com/glommio/glommio) with io_uring dependencies updated; will be deprecated if 0.10 lands under the canonical glommio name. "

MODE="${1:-package}"

# The republish carries its own patch number: fixes can ship to crates.io
# consumers without moving the version this fork presents upstream, which
# tracks glommio/glommio.
NG_VERSION="${NG_VERSION:-$(sed -n 's/^version = "\(.*\)"$/\1/p' glommio/Cargo.toml | head -1)}"

# The files this script rewrites. Backed up byte for byte before anything is
# touched, and put back on exit -- rather than `git checkout --`, which
# restores what was last committed and so silently destroys any uncommitted
# work in these files. That has bitten twice.
TOUCHED=(glommio/Cargo.toml examples/Cargo.toml README.md glommio-macros/Cargo.toml)
BACKUP="$(mktemp -d)"

for file in "${TOUCHED[@]}"; do
  mkdir -p "$BACKUP/$(dirname "$file")"
  cp "$file" "$BACKUP/$file"
done

restore() {
  for file in "${TOUCHED[@]}"; do
    cp "$BACKUP/$file" "$file" 2>/dev/null || true
  done
  rm -rf "$BACKUP"
  rm -f glommio/README.md
}
trap restore EXIT

# --- glommio/Cargo.toml -------------------------------------------------
python3 - "$NG_NAME" "$NG_REPO" "$NG_BLURB" "$NG_VERSION" <<'PY'
import re, sys

def sub1(pattern, repl, s, what):
    new_s, n = re.subn(pattern, repl, s, count=1)
    if n == 0:
        print(f"prep-ng-release: substitution failed, no match: {what}", file=sys.stderr)
        raise SystemExit(1)
    return new_s

name, repo, blurb, version = sys.argv[1:5]
p = "glommio/Cargo.toml"
s = open(p).read()

s = sub1(r'(?m)^name = "glommio"$', f'name = "{name}"', s, "glommio/Cargo.toml name rename")
s = sub1(r'(?m)^version = ".*"$', f'version = "{version}"', s, "glommio/Cargo.toml version rewrite")
s = sub1(r'(?m)^readme = .*$', 'readme = "README.md"', s, "glommio/Cargo.toml readme rewrite")
s = sub1(r'(?m)^repository = .*$', f'repository = "{repo}"', s, "glommio/Cargo.toml repository rewrite")
s = sub1(r'(?m)^homepage = .*$', f'homepage = "{repo}"', s, "glommio/Cargo.toml homepage rewrite")
s = sub1(r'(?m)^description = "', f'description = "{blurb}', s, "glommio/Cargo.toml description rewrite")

# No [lib] rename: consumers get `use glommio::...` via cargo's own
# `package = "glommio-ng"` renaming, and a duplicate lib name would only make
# glommio and glommio-ng un-coexistable in one dependency graph.
open(p, "w").write(s)

# --- examples/Cargo.toml: path dep must follow the rename ---------------
p = "examples/Cargo.toml"
s = open(p).read()
old = 'glommio        = { path = "../glommio" }'
new = f'glommio        = {{ path = "../glommio", package = "{name}" }}'
if old not in s:
    print("prep-ng-release: substitution failed, no match: examples/Cargo.toml path dep rewrite", file=sys.stderr)
    raise SystemExit(1)
s = s.replace(old, new)
open(p, "w").write(s)
PY

# --- glommio-macros: its own name, and the dependency that points at it ----
python3 - "$NG_NAME" "$NG_VERSION" <<'PY'
import re, sys

def sub1(pattern, repl, s, what):
    new_s, n = re.subn(pattern, repl, s, count=1)
    if n == 0:
        print(f"prep-ng-release: substitution failed, no match: {what}", file=sys.stderr)
        raise SystemExit(1)
    return new_s

name, version = sys.argv[1:3]

p = "glommio-macros/Cargo.toml"
s = open(p).read()
s = sub1(r'(?m)^name = "glommio-macros"$', f'name = "{name}-macros"', s,
          "glommio-macros/Cargo.toml name rename")
s = sub1(r'(?m)^version = ".*"$', f'version = "{version}"', s,
          "glommio-macros/Cargo.toml version rewrite")
open(p, "w").write(s)

p = "glommio/Cargo.toml"
s = open(p).read()
s = sub1(
    r'(?m)^glommio-macros(\s*)= \{ version = "[^"]*", path = "\.\./glommio-macros"',
    f'glommio-macros\\1= {{ version = "{version}", package = "{name}-macros", '
    f'path = "../glommio-macros"',
    s,
    "glommio/Cargo.toml glommio-macros dependency rewrite",
)
open(p, "w").write(s)
PY

# --- README: packaged copy + stopgap notice -----------------------------
python3 - <<'PY'
notice = """> **Note — this is `glommio-ng`, a stopgap republish.**
>
> The canonical [`glommio`](https://crates.io/crates/glommio) crate last
> published 0.9.0 in March 2024, and 0.9.0 no longer compiles against current
> kernel headers. This crate exists so downstreams can depend on a working
> release instead of a git revision. It is published under a different crate
> name; cargo's dependency renaming keeps `use glommio::...` working, so only
> your `Cargo.toml` line differs:
>
> ```toml
> glommio = { package = "glommio-ng", version = "0.10" }
> ```
>
> `glommio-ng` and canonical `glommio` are **different crates whose types do not
> unify**: a library compiled against one cannot accept the other's executor,
> tasks or sockets, even where the source is identical. That is the cost of this
> fork existing. It will be deprecated in favour of the canonical name if 0.10
> can be published there.

"""
s = open("README.md").read()
open("glommio/README.md", "w").write(notice + s)
PY

# glommio-ng-macros has no unpublished dependencies and must publish first;
# glommio-ng depends on it and cannot verify until it is on crates.io.
# The macro crate goes first everywhere below, and not only in the publish
# step: until it exists on crates.io at this version, the main crate cannot
# even be packaged, because packaging resolves the published dependency rather
# than the path one. So `--publish` publishes the macro crate before the main
# crate is packaged at all, and `--dry-run` cannot fully verify the main crate
# until a real publish of the macro crate has happened.
echo "== cargo package: $NG_NAME-macros $NG_VERSION =="
cargo package -p "$NG_NAME-macros" --allow-dirty

if [ "$MODE" = "--publish" ]; then
  echo "== cargo publish: $NG_NAME-macros $NG_VERSION =="
  cargo publish -p "$NG_NAME-macros" --allow-dirty
elif [ "$MODE" = "--dry-run" ]; then
  echo "== cargo publish --dry-run: $NG_NAME-macros =="
  cargo publish -p "$NG_NAME-macros" --allow-dirty --dry-run
fi

echo "== cargo package: $NG_NAME $NG_VERSION =="
cargo package -p "$NG_NAME" --allow-dirty

TARBALL="target/package/$NG_NAME-$NG_VERSION.crate"
echo
echo "== tarball =="
ls -lh "$TARBALL"
echo "files: $(tar tzf "$TARBALL" | wc -l)"

if [ "$MODE" = "--dry-run" ]; then
  echo "== cargo publish --dry-run: $NG_NAME =="
  cargo publish -p "$NG_NAME" --allow-dirty --dry-run
elif [ "$MODE" = "--publish" ]; then
  echo "== cargo publish: $NG_NAME $NG_VERSION =="
  cargo publish -p "$NG_NAME" --allow-dirty
fi
