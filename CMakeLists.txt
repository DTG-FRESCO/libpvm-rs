cmake_minimum_required(VERSION 2.8.9)

SET(LIB_FILE ${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}pvm${CMAKE_SHARED_LIBRARY_SUFFIX})
SET(LIB_HEADER_FOLDER ${CMAKE_CURRENT_SOURCE_DIR}/src/include)
set(PVMAPP "${CMAKE_CURRENT_BINARY_DIR}/pvm")
set(CMAKE_C_FLAGS "-std=c99")

file(GLOB_RECURSE RUST_SRCS "${CMAKE_SOURCE_DIR}/src/*.rs")

add_custom_target(
    libpvm-clean
    COMMAND cargo clean
    WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}"
    VERBATIM)

add_custom_command(
    OUTPUT ${LIB_FILE} ${PVMAPP}
    COMMAND cargo build --release --out-dir=${CMAKE_CURRENT_BINARY_DIR} -Z unstable-options
    WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}"
    DEPENDS ${RUST_SRCS}
    VERBATIM)

add_custom_target(libpvm_target DEPENDS ${LIB_FILE})

add_library(libpvm STATIC IMPORTED GLOBAL)
add_dependencies(libpvm libpvm_target)

set_target_properties(libpvm
    PROPERTIES
    IMPORTED_LOCATION ${LIB_FILE}
    INTERFACE_INCLUDE_DIRECTORIES ${LIB_HEADER_FOLDER})

add_executable(cpvmuser src/cpvmuser.c)

target_link_libraries(cpvmuser libpvm)
