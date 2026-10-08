#!/usr/bin/env bash
# Copyright (C) 2026 EloqData Inc. SPDX-License-Identifier: Apache-2.0
# Publish a verified, runnable package once; existing demo volumes keep that build.
set -euo pipefail
release_root=/release
mkdir -p "$release_root"
# One-shot installers may race; flock is released even if a download is killed.
exec 9>"$release_root/.install.lock"
flock -n 9 || { echo 'Another quick-start download is running; retry when it finishes.' >&2; exit 1; }
tag=${LAVIK_QUICKSTART_VERSION:-nightly}
[[ $tag =~ ^(nightly|bundled|v[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9.-]+)?)$ ]] || {
  echo 'Choose nightly or a published version tag using LAVIK_QUICKSTART_VERSION.' >&2; exit 1;
}
case $(uname -m) in
  x86_64|amd64) arch=x86_64 ;;
  aarch64|arm64) arch=aarch64 ;;
  *) echo 'Quick start supports Linux AMD64 and ARM64.' >&2; exit 1 ;;
esac
validate_runtime() {
  local package=$1
  for binary in lavik lavik-meta lavik-ctl runtime/bin/node; do
    [[ -x "$package/$binary" ]] || { echo "Release is missing $binary; use a current release with bundled Admin." >&2; return 1; }
  done
  "$package/runtime/bin/node" -e 'const [major,minor] = process.versions.node.split(".").map(Number); if(major < 24 || (major === 24 && minor < 15)) process.exit(1)'
  local help
  help=$("$package/lavik" --help)
  [[ $help == *--meta-seed* ]] || { echo 'This release predates the demo cluster interface; use nightly or a newer release.' >&2; return 1; }
}
if [[ -e "$release_root/current" || -L "$release_root/current" ]]; then
  [[ $(cat "$release_root/current/.quickstart-tag") == "$tag" && $(cat "$release_root/current/.quickstart-arch") == "$arch" ]] || {
    echo 'This demo already has a different release or architecture. Keep its original version; use a separate demo for another release.' >&2; exit 1;
  }
  validate_runtime "$release_root/current"
  printf 'Reusing Lavik %s (revision %s).\n' "$tag" "$(cat "$release_root/current/REVISION")"
  exit 0
fi
# A missing release with populated volumes can mean an older source-built demo
# or a lost release volume. Never guess which binary can recover that state.
if [[ -d /existing && -n $(find /existing -mindepth 2 -type f -print -quit) ]]; then
  echo 'Existing demo data has no retained release. Restore its original binaries or use a separate demo; no data was changed.' >&2
  exit 1
fi
stage=$(mktemp -d "$release_root/.download-XXXXXX")
trap 'rm -rf "$stage"' EXIT
if [[ $tag == bundled ]]; then
  [[ -d /bundled && -f /bundled/REVISION ]] || { echo 'The packaged release is missing; restore the extracted archive and retry.' >&2; exit 1; }
  validate_runtime /bundled
  mkdir "$stage/package"
  cp -a /bundled/. "$stage/package/"
  (cd "$stage/package" && sha256sum lavik lavik-meta lavik-ctl runtime/bin/node VERSION REVISION > .quickstart-bundle.sha256)
  printf '%s\n' "$tag" > "$stage/package/.quickstart-tag"
  printf '%s\n' "$arch" > "$stage/package/.quickstart-arch"
  mv "$stage/package" "$release_root/current"
  printf 'Installed bundled Lavik (revision %s).\n' "$(cat "$release_root/current/REVISION")"
  exit 0
fi
archive="lavik-$tag-linux-$arch-minimal.tar.gz"
url="https://github.com/eloqdata/lavik/releases/download/$tag/$archive"
printf 'Downloading Lavik %s for %s…\n' "$tag" "$arch"
fetch_release() {
  local source=$1 destination=$2 timeout=$3
  # curl's default retry set excludes TLS handshakes and connection resets.
  # These are idempotent GETs into staging files, so retry all transfer errors;
  # curl truncates the partial output before retrying. Keep TLS verification on.
  if curl --fail --location --proto '=https' --proto-redir '=https' \
      --retry 3 --retry-all-errors --retry-max-time "$timeout" \
      --connect-timeout 30 --max-time "$timeout" "$source" -o "$destination"; then
    return 0
  else
    local status=$?
    printf '\nRelease download failed (curl exit %s). No release was installed.\n' "$status" >&2
    echo 'Retry demo setup in Admin (or rerun setup.sh for the legacy quick start). If this persists, check Docker Desktop proxy/VPN settings and HTTPS access to github.com and release-assets.githubusercontent.com.' >&2
    return "$status"
  fi
}
fetch_release "$url.sha256" "$stage/checksum" 120
read -r digest checksum_name extra < "$stage/checksum"
[[ $digest =~ ^[a-f0-9]{64}$ && ${checksum_name#\*} == "$archive" && -z $extra && $(wc -l < "$stage/checksum") -le 1 ]] || {
  echo 'Invalid published release checksum.' >&2; exit 1;
}
fetch_release "$url" "$stage/$archive" 600
(cd "$stage" && printf '%s  %s\n' "$digest" "$archive" | sha256sum --check -) || {
  echo 'Release verification failed. Nightly may have changed during download; rerun setup.' >&2; exit 1;
}
mkdir "$stage/package"
tar -xzf "$stage/$archive" -C "$stage/package" --strip-components=1 --no-same-owner
validate_runtime "$stage/package"
printf '%s\n' "$tag" > "$stage/package/.quickstart-tag"
printf '%s\n' "$arch" > "$stage/package/.quickstart-arch"
printf '%s\n' "$digest" > "$stage/package/.quickstart-sha256"
# The directory rename publishes all files together. Never overwrite a package
# that running Meta/Data processes or retained storage volumes may depend on.
mv "$stage/package" "$release_root/current"
printf 'Verified Lavik %s (revision %s).\n' "$tag" "$(cat "$release_root/current/REVISION")"
