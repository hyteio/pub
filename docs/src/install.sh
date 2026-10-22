#!/bin/sh
# Installs pub from its GitHub releases:
#   curl -fsSL https://hyteio.github.io/pub/install.sh | sh
#
# PUB_VERSION      a version such as 1.0.1 (default: the latest release)
# PUB_INSTALL_DIR  where to put the binary (default: ~/.local/bin)
# PUB_DOWNLOAD_URL where releases are downloaded from, for a mirror
#                  (default: https://github.com/hyteio/pub/releases/download)
set -eu

repo=https://github.com/hyteio/pub
base=${PUB_DOWNLOAD_URL:-$repo/releases/download}
dir=${PUB_INSTALL_DIR:-$HOME/.local/bin}

fail() { echo "pub install: $*" >&2; exit 1; }

case "$(uname -s)" in
    Linux) os=unknown-linux-musl ;;
    Darwin) os=apple-darwin ;;
    *) fail "no release for $(uname -s); build from source: $repo" ;;
esac
case "$(uname -m)" in
    x86_64 | amd64) arch=x86_64 ;;
    arm64 | aarch64) arch=aarch64 ;;
    *) fail "no release for $(uname -m); build from source: $repo" ;;
esac

if [ -n "${PUB_VERSION:-}" ]; then
    tag=v${PUB_VERSION#v}
else
    # The latest release's URL redirects to its tag; no API call, so no rate limit.
    tag=$(curl -fsSI "$repo/releases/latest" | tr -d '\r' |
        sed -n 's|^[Ll]ocation: .*/releases/tag/\(v[^/]*\)$|\1|p')
    [ -n "$tag" ] || fail "couldn't find the latest release"
fi

name=pub-$tag-$arch-$os
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

echo "downloading $name"
curl -fsSL -o "$tmp/$name.tar.gz" "$base/$tag/$name.tar.gz" || fail "download failed: $base/$tag/$name.tar.gz"
curl -fsSL -o "$tmp/$name.tar.gz.sha256" "$base/$tag/$name.tar.gz.sha256" || fail "checksum download failed"

if command -v sha256sum >/dev/null 2>&1; then
    check="sha256sum -c"
else
    check="shasum -a 256 -c"
fi
(cd "$tmp" && $check "$name.tar.gz.sha256" >/dev/null 2>&1) || fail "checksum mismatch for $name.tar.gz"

tar -xzf "$tmp/$name.tar.gz" -C "$tmp"
mkdir -p "$dir"
install -m 0755 "$tmp/$name/pub" "$dir/pub"
echo "installed $("$dir/pub" --version) to $dir/pub"

case ":$PATH:" in
    *":$dir:"*) ;;
    *) echo "add $dir to your PATH to run pub" ;;
esac
