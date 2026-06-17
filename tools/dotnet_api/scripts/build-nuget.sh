#!/usr/bin/env bash
# ---------------------------------------------------------------
# build-nuget.sh  —  Assemble native binaries and pack a NuGet.
#
# Usage:
#   ./build-nuget.sh [--version 0.12.0-vela.abc1234]
#
# Expects native binaries to already be placed under:
#   src/Kuzu.Net/runtimes/{rid}/native/
#
# Produces: *.nupkg in src/Kuzu.Net/bin/Release/
# ---------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$SCRIPT_DIR/../src/Kuzu.Net"

VERSION=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --version) VERSION="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

cd "$PROJECT_DIR"

# Verify at least one native runtime is present
if [ ! -d "runtimes" ] || [ -z "$(ls -A runtimes/ 2>/dev/null)" ]; then
    echo "ERROR: No native runtimes found under $PROJECT_DIR/runtimes/"
    echo "Expected structure:"
    echo "  runtimes/linux-x64/native/libkuzunet.so + libkuzu.so"
    echo "  runtimes/linux-arm64/native/libkuzunet.so + libkuzu.so"
    echo "  runtimes/osx-arm64/native/libkuzunet.dylib + libkuzu.dylib"
    echo "  runtimes/win-x64/native/kuzunet.dll + kuzu_shared.dll"
    exit 1
fi

echo "=== Native runtimes found ==="
find runtimes -type f | sort

PACK_ARGS=("-c" "Release")
if [ -n "$VERSION" ]; then
    PACK_ARGS+=("-p:Version=$VERSION")
fi

echo ""
echo "=== Packing NuGet ==="
dotnet pack "${PACK_ARGS[@]}"

echo ""
echo "=== Package created ==="
find bin/Release -name "*.nupkg" -o -name "*.snupkg" 2>/dev/null
