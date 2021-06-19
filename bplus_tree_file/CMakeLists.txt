cmake_minimum_required(VERSION 3.15)
project(bplus_tree_file C)

set(CMAKE_C_STANDARD 11)

include_directories(include)

add_executable(bplus_tree_file
        examples/bf_main.c
        examples/main1.c
        examples/main2.c
        examples/main3.c
        include/AM.h
        include/bf.h
        include/defn.h
        src/AM.c include/functions.h src/functions.c include/DefsxStructs.h)
