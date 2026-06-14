import re
import sys
from pathlib import Path

PROJECT_VERSION_PATTERN = re.compile(r'project\(Kuzu VERSION (.*?) LANGUAGES CXX C\)')
EXTENSION_VERSION_PATTERN = re.compile(r'set\(KUZU_EXTENSION_VERSION "(.*?)"\)')
PROJECT_VERSION_VALUE = "${CMAKE_PROJECT_VERSION}"


def extract_extension_version(cmake_lists_path):
    project_version = None
    extension_version = None
    with Path(cmake_lists_path).open() as cmake_lists_file:
        for line in cmake_lists_file:
            project_match = PROJECT_VERSION_PATTERN.search(line)
            if project_match:
                project_version = project_match.group(1)
            extension_match = EXTENSION_VERSION_PATTERN.search(line)
            if extension_match:
                extension_version = extension_match.group(1)
    if project_version and (not extension_version or extension_version == PROJECT_VERSION_VALUE):
        return project_version
    if extension_version:
        return extension_version
    raise RuntimeError("Failed to infer Kuzu extension version from CMakeLists.txt")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        cmake_lists_path = Path(sys.argv[1])
    else:
        cmake_lists_path = Path(__file__).resolve().parent.parent / "CMakeLists.txt"
    print(extract_extension_version(cmake_lists_path))
