#!/usr/bin/env bash
# ---------------------------------------------------------------
# build-local-osx.sh  —  Build Kuzu .NET bindings locally on macOS.
#
# Builds kuzu + libkuzunet from source (no SWIG required — uses the
# pre-generated kuzu_wrap.cpp), then places both native libraries in
# the .NET project so tests can run.
#
# Prerequisites: cmake, Xcode Command Line Tools, .NET 8+ SDK
#
# Usage:
#   ./build-local-osx.sh                    # build from source
#   ./build-local-osx.sh --prebuilt DIR     # use pre-built libkuzu from DIR
# ---------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DOTNET_API_DIR="$SCRIPT_DIR/.."
KUZU_REPO_DIR="$DOTNET_API_DIR/../.."

PREBUILT_DIR=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --prebuilt) PREBUILT_DIR="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# Detect architecture
ARCH=$(uname -m)
case "$ARCH" in
    arm64)  RID="osx-arm64" ;;
    x86_64) RID="osx-x64" ;;
    *)      echo "Unsupported arch: $ARCH"; exit 1 ;;
esac

echo "=== Building Kuzu .NET for macOS $ARCH ($RID) ==="

if [ -n "$PREBUILT_DIR" ]; then
    # ---------------------------------------------------------------
    # Mode A: Use pre-built libkuzu (out-of-tree kuzunet build)
    # ---------------------------------------------------------------
    echo "--- Using pre-built kuzu from $PREBUILT_DIR ---"

    if [ ! -f "$PREBUILT_DIR/libkuzu.dylib" ]; then
        echo "ERROR: $PREBUILT_DIR/libkuzu.dylib not found"
        exit 1
    fi

    BUILD_DIR="/tmp/kuzu-dotnet-build/kuzunet-build-$ARCH"
    mkdir -p "$BUILD_DIR"

    echo "--- Building libkuzunet.dylib ---"
    cd "$BUILD_DIR"
    cmake "$DOTNET_API_DIR" \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_OSX_ARCHITECTURES="$ARCH" \
        -DKUZU_INCLUDE_DIR="$PREBUILT_DIR" \
        -DKUZU_LIB_DIR="$PREBUILT_DIR"

    cmake --build . --config Release

    KUZUNET_LIB="$BUILD_DIR/libkuzunet.dylib"
    KUZU_LIB="$PREBUILT_DIR/libkuzu.dylib"
else
    # ---------------------------------------------------------------
    # Mode B: Build from source (default)
    # ---------------------------------------------------------------
    echo "--- Building kuzu + kuzunet from source ---"
    NUM_THREADS=$(sysctl -n hw.ncpu)

    cd "$KUZU_REPO_DIR"
    make dotnet NUM_THREADS=$NUM_THREADS EXTRA_CMAKE_FLAGS="-DCMAKE_OSX_ARCHITECTURES=$ARCH"

    KUZUNET_LIB="$KUZU_REPO_DIR/build/release/tools/dotnet_api/libkuzunet.dylib"
    KUZU_LIB="$KUZU_REPO_DIR/build/release/src/libkuzu.dylib"

    if [ ! -f "$KUZUNET_LIB" ]; then
        echo "ERROR: Expected $KUZUNET_LIB not found after build"
        echo "Searching for built libraries..."
        find "$KUZU_REPO_DIR/build" -name "libkuzunet*" -o -name "libkuzu.*" 2>/dev/null | head -10
        exit 1
    fi
fi

echo "Built: $KUZUNET_LIB"

# ---------------------------------------------------------------
# Place native libs in the .NET project
# ---------------------------------------------------------------
NATIVE_DIR="$DOTNET_API_DIR/src/Kuzu.Net/runtimes/$RID/native"
mkdir -p "$NATIVE_DIR"

cp "$KUZUNET_LIB" "$NATIVE_DIR/"
cp "$KUZU_LIB" "$NATIVE_DIR/"

echo "Installed native libs to: $NATIVE_DIR/"
ls -la "$NATIVE_DIR/"

# ---------------------------------------------------------------
# Build the .NET project
# ---------------------------------------------------------------
echo ""
echo "--- Building .NET project ---"
cd "$DOTNET_API_DIR/src"
dotnet build Kuzu.Net.sln -c Release

echo ""
echo "=== Build complete ==="
echo ""
echo "To run tests:"
echo "  cd $DOTNET_API_DIR/src && dotnet test Kuzu.Net.Tests/Kuzu.Net.Tests.csproj"
echo ""
echo "To pack NuGet:"
echo "  $DOTNET_API_DIR/scripts/build-nuget.sh"
