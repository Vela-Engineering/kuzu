import argparse
import urllib.error
import urllib.request
from pathlib import Path

BASE_URL = "https://vela-engineering.github.io/kuzu"
PLATFORMS = ["linux_amd64", "linux_arm64", "osx_amd64", "osx_arm64", "win_amd64"]
EXTENSION_PATHS = ["fts/libfts.kuzu_extension"]


def preserve_artifact(output_dir, version, platform, extension_path):
    target = output_dir / version / platform / extension_path
    if target.exists():
        return
    url = "/".join([BASE_URL, version, platform, extension_path])
    try:
        with urllib.request.urlopen(url) as response:
            data = response.read()
    except urllib.error.HTTPError as err:
        if err.code == 404:
            return
        raise
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(data)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="public")
    parser.add_argument("--releases", default="scripts/extension/PRODUCTION_RELEASES")
    args = parser.parse_args()
    output_dir = Path(args.output)
    releases = Path(args.releases).read_text().splitlines()
    for version in releases:
        if not version:
            continue
        for platform in PLATFORMS:
            for extension_path in EXTENSION_PATHS:
                preserve_artifact(output_dir, version, platform, extension_path)


if __name__ == "__main__":
    main()
