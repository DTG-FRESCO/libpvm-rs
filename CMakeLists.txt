cmake_minimum_required(VERSION 2.8.9)

add_custom_target(
    libpvm
    COMMAND cargo build --release
    WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}"
    VERBATIM)

add_custom_target(
    libpvm-clean
    COMMAND cargo clean
    WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}"
    VERBATIM)

include_directories(src/include)

add_executable(cpvmuser src/cpvmuser.c)
add_executable(pvm2csv src/pvm2csv.c)
add_executable(nullpvm src/nullpvm.c)
add_executable(pvm2proc src/pvm2proc.c)
add_executable(pvmdbg src/pvmdbg.c)

add_dependencies(cpvmuser libpvm)
add_dependencies(pvm2csv libpvm)
add_dependencies(nullpvm libpvm)
add_dependencies(pvm2proc libpvm)
add_dependencies(pvmdbg libpvm)

set(RUST_TARGET_DIR "${CMAKE_SOURCE_DIR}/target/release")
set(LIBPVM "${RUST_TARGET_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}pvm${CMAKE_SHARED_LIBRARY_SUFFIX}")
set(CMAKE_C_FLAGS "-std=c99")

target_link_libraries(cpvmuser ${LIBPVM})
target_link_libraries(pvm2csv ${LIBPVM})
target_link_libraries(nullpvm ${LIBPVM})
target_link_libraries(pvm2proc ${LIBPVM})
target_link_libraries(pvmdbg ${LIBPVM})
