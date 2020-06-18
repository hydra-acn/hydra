cmake_minimum_required(VERSION 2.8)
project (FakeRand)

include_directories(
    src
)

add_library(fakerand STATIC
    src/fakerand.h
    src/fakerand.c
)

set_target_properties(fakerand PROPERTIES LINKER_LANGUAGE C)

install(
    TARGETS fakerand
    ARCHIVE DESTINATION lib
)
