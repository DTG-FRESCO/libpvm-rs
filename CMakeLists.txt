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

add_executable(copususer src/copususer.c)
add_executable(pvm2csv src/pvm2csv.c)
add_executable(nullpvm src/nullpvm.c)
add_executable(pvm2proc src/pvm2proc.c)

add_dependencies(copususer libpvm)
add_dependencies(pvm2csv libpvm)
add_dependencies(nullpvm libpvm)
add_dependencies(pvm2proc libpvm)

set(RUST_TARGET_DIR "${CMAKE_SOURCE_DIR}/target/release")
set(LIBOPUS "${RUST_TARGET_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}opus${CMAKE_SHARED_LIBRARY_SUFFIX}")

target_link_libraries(copususer ${LIBOPUS})
target_link_libraries(pvm2csv ${LIBOPUS})
target_link_libraries(nullpvm ${LIBOPUS})
target_link_libraries(pvm2proc ${LIBOPUS})
