#!/bin/sh
# Writes the Homebrew formula for a release to stdout:
#   homebrew-formula.sh 1.0.1 dist/
# where dist/ holds the release's pub-v<version>-<target>.tar.gz.sha256 files.
set -eu
version=$1
dist=$2
url=https://github.com/hyteio/pub/releases/download/v$version

sha() {
    file="$dist/pub-v$version-$1.tar.gz.sha256"
    [ -s "$file" ] || { echo "missing $file" >&2; exit 1; }
    cut -d' ' -f1 "$file"
}

block() {
    cat <<RUBY
    on_$1 do
      url "$url/pub-v$version-$2.tar.gz"
      sha256 "$(sha "$2")"
    end
RUBY
}

cat <<RUBY
class Pub < Formula
  desc "Send, receive, browse and move messages on message brokers"
  homepage "https://hyteio.github.io/pub/"
  version "$version"
  license "Apache-2.0"

  on_macos do
$(block arm aarch64-apple-darwin)
$(block intel x86_64-apple-darwin)
  end

  on_linux do
$(block arm aarch64-unknown-linux-musl)
$(block intel x86_64-unknown-linux-musl)
  end

  def install
    bin.install "pub"
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/pub --version")
  end
end
RUBY
