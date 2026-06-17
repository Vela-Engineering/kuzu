cmake_minimum_required(VERSION 3.15)
project(kuzunet C CXX)

# --------------------------------------------------------------------------
# Options
# --------------------------------------------------------------------------
option(DOTNET_REGENERATE_SWIG
    "Run SWIG to regenerate kuzu_wrap.cpp and C# classes (requires swig on PATH)"
    OFF)

set(KUZU_INCLUDE_DIR "" CACHE PATH
    "Path to directory containing kuzu.h (for out-of-tree builds)")
set(KUZU_LIB_DIR "" CACHE PATH
    "Path to directory containing the kuzu shared library (for out-of-tree builds)")

# --------------------------------------------------------------------------
# Locate kuzu headers and library
# --------------------------------------------------------------------------
# When built as part of the kuzu tree the include path and kuzu_shared target
# are already available. For standalone / out-of-tree builds the caller must
# supply KUZU_INCLUDE_DIR and KUZU_LIB_DIR.
if(TARGET kuzu_shared)
    # In-tree build – headers and library come from the parent project.
    set(_kuzu_include_dir "${PROJECT_SOURCE_DIR}/../../src/include"
                          "${CMAKE_BINARY_DIR}/src/include")
    set(_kuzu_link_target kuzu_shared)
else()
    # Out-of-tree build – use caller-provided paths.
    if(NOT KUZU_INCLUDE_DIR)
        message(FATAL_ERROR "KUZU_INCLUDE_DIR must be set for out-of-tree builds")
    endif()
    if(NOT KUZU_LIB_DIR)
        message(FATAL_ERROR "KUZU_LIB_DIR must be set for out-of-tree builds")
    endif()

    set(_kuzu_include_dir "${KUZU_INCLUDE_DIR}")

    # Create an imported target for the pre-built kuzu library.
    add_library(kuzu_imported SHARED IMPORTED)
    if(WIN32)
        set_target_properties(kuzu_imported PROPERTIES
            IMPORTED_IMPLIB "${KUZU_LIB_DIR}/kuzu_shared.lib"
            IMPORTED_LOCATION "${KUZU_LIB_DIR}/kuzu_shared.dll")
    elseif(APPLE)
        set_target_properties(kuzu_imported PROPERTIES
            IMPORTED_LOCATION "${KUZU_LIB_DIR}/libkuzu.dylib")
    else()
        set_target_properties(kuzu_imported PROPERTIES
            IMPORTED_LOCATION "${KUZU_LIB_DIR}/libkuzu.so")
    endif()
    set(_kuzu_link_target kuzu_imported)
endif()

# --------------------------------------------------------------------------
# Optional: SWIG regeneration
# --------------------------------------------------------------------------
set(SWIG_INTERFACE "${CMAKE_CURRENT_SOURCE_DIR}/swig/kuzu.i")
set(WRAP_SOURCE    "${CMAKE_CURRENT_SOURCE_DIR}/swig/generated/kuzu_wrap.cpp")
set(CSHARP_OUTDIR  "${CMAKE_CURRENT_SOURCE_DIR}/src/Kuzu.Net/Generated")

if(DOTNET_REGENERATE_SWIG)
    find_program(SWIG_EXECUTABLE swig REQUIRED)
    message(STATUS "SWIG found: ${SWIG_EXECUTABLE}")

    # Determine the platform include flag for SWIG.
    if(WIN32)
        set(SWIG_PLATFORM_DEFS "-D_WIN32" "-DKUZU_C_API=__declspec(dllimport)")
    else()
        set(SWIG_PLATFORM_DEFS "-DKUZU_C_API=__attribute__((visibility(\"default\")))")
    endif()

    add_custom_command(
        OUTPUT  ${WRAP_SOURCE}
        COMMAND ${CMAKE_COMMAND} -E make_directory "${CSHARP_OUTDIR}"
        COMMAND ${SWIG_EXECUTABLE}
            -c++ -csharp
            ${SWIG_PLATFORM_DEFS}
            -I${_kuzu_include_dir}
            -outdir "${CSHARP_OUTDIR}"
            -o "${WRAP_SOURCE}"
            "${SWIG_INTERFACE}"
        DEPENDS ${SWIG_INTERFACE}
        COMMENT "Generating SWIG wrapper for Kuzu .NET"
    )
endif()

# --------------------------------------------------------------------------
# Build the native wrapper shared library
# --------------------------------------------------------------------------
add_library(${PROJECT_NAME} SHARED ${WRAP_SOURCE})
target_include_directories(${PROJECT_NAME} PRIVATE ${_kuzu_include_dir})
target_link_libraries(${PROJECT_NAME} PRIVATE ${_kuzu_link_target})

# Platform-specific settings
if(APPLE)
    set_target_properties(${PROJECT_NAME} PROPERTIES
        INSTALL_RPATH "@loader_path"
        BUILD_WITH_INSTALL_RPATH TRUE)
elseif(UNIX)
    set_target_properties(${PROJECT_NAME} PROPERTIES
        INSTALL_RPATH "$ORIGIN"
        BUILD_WITH_INSTALL_RPATH TRUE)
endif()

# --------------------------------------------------------------------------
# Install
# --------------------------------------------------------------------------
install(TARGETS ${PROJECT_NAME}
    LIBRARY DESTINATION lib
    RUNTIME DESTINATION bin)
