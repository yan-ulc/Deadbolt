#!/usr/bin/env bash
set -euo pipefail

# Package exactly one CLI/runtime release. The caller supplies the immutable
# digest produced by the existing staging image publisher; this script never
# builds or publishes a second control-plane image.
if [[ $# -ne 3 ]]; then
  echo "usage: $0 <runtime-binary> <control-plane-image@sha256:digest> <output-dir>" >&2
  exit 2
fi

runtime_binary=$1
control_plane_image=$2
output_dir=$3
repo_root=$(cd "$(dirname "$0")/.." && pwd)

case "$control_plane_image" in
  *@sha256:*) ;;
  *) echo "control-plane image must be an immutable @sha256 reference" >&2; exit 2 ;;
esac

rm -rf "$output_dir"
mkdir -p "$output_dir/bin" "$output_dir/share/deadbolt/runner"
cp "$runtime_binary" "$output_dir/bin/runtime"
cp "$repo_root/deploy/compose/standalone-compose.yaml" "$output_dir/share/deadbolt/compose.yaml"
cp "$repo_root/runner/node/dist/"*.js "$output_dir/share/deadbolt/runner/"
printf '{"type":"module"}\n' > "$output_dir/share/deadbolt/runner/package.json"

commit=$(git -C "$repo_root" rev-parse HEAD)
version=$(git -C "$repo_root" describe --tags --always)
printf '{"version":"%s","commit":"%s","controlPlaneImage":"%s"}\n' "$version" "$commit" "$control_plane_image" > "$output_dir/share/deadbolt/release.json"
