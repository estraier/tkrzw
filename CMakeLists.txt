cmake_minimum_required(VERSION 3.28)

cmake_policy(SET CMP0048 NEW)
cmake_policy(SET CMP0091 NEW)

set(PKG_VERSION 1.0.31)
set(LIB_VERSION 1.74.0)

project(tkrzw
  VERSION ${PKG_VERSION}
  DESCRIPTION "a set of implementations of DBM"
  HOMEPAGE_URL "https://dbmx.net/tkrzw/"
  LANGUAGES C CXX
)
add_library(tkrzw)
set_target_properties(tkrzw PROPERTIES VERSION ${LIB_VERSION} SOVERSION 1)

include(CMakePackageConfigHelpers)
include(CPack)
include(GNUInstallDirs)
include(CMakeDependentOption)

option(TKRZW_TOOLS "Build cli frontends" OFF)
cmake_dependent_option(TKRZW_TESTS "Generate a test target" OFF "TKRZW_TOOLS" OFF)


find_package(Threads)
target_link_libraries(tkrzw PUBLIC Threads::Threads)

# See https://gitlab.kitware.com/cmake/cmake/-/issues/18837
if(MSVC)
	target_compile_options(tkrzw PUBLIC "/Zc:__cplusplus")
endif()

target_compile_definitions(tkrzw
    PRIVATE
        _FILE_OFFSET_BITS=64
        _TKRZW_PKG_VERSION="${CMAKE_PROJECT_VERSION}"
        _TKRZW_LIB_VERSION="${LIB_VERSION}"
)

target_compile_features(tkrzw PUBLIC cxx_std_17)

target_include_directories(tkrzw
    INTERFACE
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/>
)
set_target_properties(tkrzw
    PROPERTIES
        MSVC_RUNTIME_LIBRARY MultiThreaded$<$<CONFIG:Debug>:Debug>$<IF:$<STREQUAL:$<TARGET_PROPERTY:tkrzw,TYPE>,SHARED_LIBRARY>,DLL,>
        DEBUG_POSTFIX "d"
        WINDOWS_EXPORT_ALL_SYMBOLS ON
)


target_sources(tkrzw PUBLIC
    FILE_SET public_headers
    TYPE HEADERS
    FILES
        tkrzw_lib_common.h
        tkrzw_str_util.h
        tkrzw_hash_util.h
        tkrzw_time_util.h
        tkrzw_thread_util.h
        tkrzw_logger.h
        tkrzw_compress.h
        tkrzw_containers.h
        tkrzw_key_comparators.h
        tkrzw_file.h
        tkrzw_file_util.h
        tkrzw_file_std.h
        tkrzw_file_mmap.h
        tkrzw_file_pos.h
        tkrzw_file_poly.h
        tkrzw_message_queue.h
        tkrzw_dbm.h
        tkrzw_dbm_ulog.h
        tkrzw_dbm_common_impl.h
        tkrzw_dbm_hash_impl.h
        tkrzw_dbm_hash.h
        tkrzw_dbm_tree_impl.h
        tkrzw_dbm_tree.h
        tkrzw_dbm_skip_impl.h
        tkrzw_dbm_skip.h
        tkrzw_dbm_tiny.h
        tkrzw_dbm_baby.h
        tkrzw_dbm_cache.h
        tkrzw_dbm_std.h
        tkrzw_dbm_poly.h
        tkrzw_dbm_shard.h
        tkrzw_dbm_async.h
        tkrzw_index.h
        tkrzw_cmd_util.h
        tkrzw_langc.h
    BASE_DIRS
        ${PROJECT_SOURCE_DIR}
)
target_sources(tkrzw PRIVATE
    tkrzw_lib_common.cc
    tkrzw_str_util.cc
    tkrzw_hash_util.cc
    tkrzw_time_util.cc
    tkrzw_compress.cc
    tkrzw_key_comparators.cc
    tkrzw_file.cc
    tkrzw_file_util.cc
    tkrzw_file_std.cc
    tkrzw_file_mmap.cc
    tkrzw_file_pos.cc
    tkrzw_file_poly.cc
    tkrzw_message_queue.cc
    tkrzw_dbm.cc
    tkrzw_dbm_ulog.cc
    tkrzw_dbm_common_impl.cc
    tkrzw_dbm_hash_impl.cc
    tkrzw_dbm_hash.cc
    tkrzw_dbm_tree_impl.cc
    tkrzw_dbm_tree.cc
    tkrzw_dbm_skip_impl.cc
    tkrzw_dbm_skip.cc
    tkrzw_dbm_tiny.cc
    tkrzw_dbm_baby.cc
    tkrzw_dbm_cache.cc
    tkrzw_dbm_std.cc
    tkrzw_dbm_poly.cc
    tkrzw_dbm_shard.cc
    tkrzw_dbm_async.cc
    tkrzw_cmd_util.cc
    tkrzw_langc.cc
)

set(TOOL_TARGETS)
if (TKRZW_TOOLS)
    function(TkrzwAddCommandLineTool)
        cmake_parse_arguments(params "" "NAME" "SOURCES" ${ARGN})
        add_executable(${params_NAME})
        target_sources(${params_NAME} PRIVATE ${params_SOURCES})
        target_link_libraries(${params_NAME} PRIVATE tkrzw)
        set_target_properties(${params_NAME} PROPERTIES MSVC_RUNTIME_LIBRARY MultiThreaded$<$<CONFIG:Debug>:Debug>$<IF:$<STREQUAL:$<TARGET_PROPERTY:tkrzw,TYPE>,SHARED_LIBRARY>,DLL,>)
        set(TOOL_TARGETS ${TOOL_TARGETS} ${params_NAME} PARENT_SCOPE)
    endfunction()

    TkrzwAddCommandLineTool(NAME tkrzw_build_util SOURCES tkrzw_build_util.cc)
    TkrzwAddCommandLineTool(NAME tkrzw_str_perf SOURCES tkrzw_str_perf.cc)
    TkrzwAddCommandLineTool(NAME tkrzw_file_perf SOURCES tkrzw_file_perf.cc)
    TkrzwAddCommandLineTool(NAME tkrzw_dbm_util SOURCES tkrzw_dbm_util.cc)
    TkrzwAddCommandLineTool(NAME tkrzw_dbm_perf SOURCES tkrzw_dbm_perf.cc)
    TkrzwAddCommandLineTool(NAME tkrzw_dbm_tran SOURCES tkrzw_dbm_tran.cc)
    TkrzwAddCommandLineTool(NAME tkrzw_ulog_util SOURCES tkrzw_ulog_util.cc)
    TkrzwAddCommandLineTool(NAME tkrzw_langc_check SOURCES tkrzw_langc_check.c)
endif (TKRZW_TOOLS)

install(
    TARGETS tkrzw ${TOOL_TARGETS}
    EXPORT ${CMAKE_PROJECT_NAME}Targets
    FILE_SET public_headers
)

install(EXPORT ${CMAKE_PROJECT_NAME}Targets DESTINATION share/cmake/${CMAKE_PROJECT_NAME})

configure_package_config_file(${CMAKE_CURRENT_SOURCE_DIR}/Config.cmake.in
  "${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_PROJECT_NAME}Config.cmake"
  INSTALL_DESTINATION share/cmake/${CMAKE_PROJECT_NAME}
  NO_SET_AND_CHECK_MACRO
  NO_CHECK_REQUIRED_COMPONENTS_MACRO
)

write_basic_package_version_file(
  "${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_PROJECT_NAME}ConfigVersion.cmake"
  VERSION "${CMAKE_PROJECT_VERSION}"
  COMPATIBILITY AnyNewerVersion
)

install(
  FILES
    "${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_PROJECT_NAME}Config.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_PROJECT_NAME}ConfigVersion.cmake"
  DESTINATION
    share/cmake/${CMAKE_PROJECT_NAME}
)




if (TKRZW_TESTS)
    enable_testing()
    add_test(NAME check-build-util:1 COMMAND tkrzw_build_util version)
    add_test(NAME check-build-util:2 COMMAND tkrzw_build_util config)
    add_test(NAME check-build-util:3 COMMAND tkrzw_build_util config -v)
    add_test(NAME check-build-util:4 COMMAND tkrzw_build_util config -i)
    add_test(NAME check-build-util:5 COMMAND tkrzw_build_util config -l)
    add_test(NAME check-build-util:6 COMMAND tkrzw_build_util config -p)

    add_test(NAME check-str-perf:1 COMMAND tkrzw_str_perf search --iter 1000 --text 2000 --pattern 5 --chars 10)
    add_test(NAME check-str-perf:2 COMMAND tkrzw_str_perf search --iter 1000 --text 2000 --pattern 5 --chars 10 --whole 3)
    add_test(NAME check-str-perf:3 COMMAND tkrzw_str_perf search --iter 1000 --text 2000 --pattern 5 --chars 10 --batch 3)
    add_test(NAME check-str-perf:4 COMMAND tkrzw_str_perf hash --iter 1000 --text 10000)
    add_test(NAME check-str-perf:5 COMMAND tkrzw_str_perf hash --iter 100 --text 1000 --error_type random)
    add_test(NAME check-str-perf:6 COMMAND tkrzw_str_perf compress --iter 200 --text 500)

    function(TkrzwRunCheckFilePerf)
        #add_test(NAME check-file-perf:1  COMMAND tkrzw_file_perf sequence --file std --iter 50000 --threads 5 --size 20 casket-check-file-perf-1.db)
        #              |- hardcoded -| ^          |--hardcoded--| |                                                    | |----  generated name ----| 
        #                              |                          |                                                    |
        #                            INDEX                        |--------------------- OPTIONS ----------------------|
        cmake_parse_arguments(params "" "INDEX" "OPTIONS" ${ARGN})
        add_test(
            NAME
                check-file-perf:${params_INDEX}
            COMMAND
                tkrzw_file_perf ${params_OPTIONS} casket-check-file-perf.${params_INDEX}.db
        )
    endfunction()

    TkrzwRunCheckFilePerf(INDEX   1 OPTIONS sequence --file std --iter 50000 --threads 5 --size 20)
    TkrzwRunCheckFilePerf(INDEX   2 OPTIONS sequence --file std --iter 50000 --threads 5 --size 20 --random)
    TkrzwRunCheckFilePerf(INDEX   3 OPTIONS sequence --file mmap-para --iter 50000 --threads 5 --size 20 --append)
    TkrzwRunCheckFilePerf(INDEX   4 OPTIONS wicked   --file mmap-para --iter 50000 --threads 5 --size 20)
    TkrzwRunCheckFilePerf(INDEX   5 OPTIONS sequence --file mmap-atom --iter 50000 --threads 5 --size 20)
    TkrzwRunCheckFilePerf(INDEX   6 OPTIONS sequence --file mmap-para --iter 50000 --threads 5 --size 20 --alloc_init 1 --alloc_inc 1.2)
    TkrzwRunCheckFilePerf(INDEX   7 OPTIONS sequence --file mmap-para --iter 50000 --threads 5 --size 20 --alloc_init 1 --alloc_inc 1.2 --random)
    TkrzwRunCheckFilePerf(INDEX   8 OPTIONS sequence --file mmap-para --iter 50000 --threads 5 --size 20 --alloc_init 1 --alloc_inc 1.2 --append)
    TkrzwRunCheckFilePerf(INDEX   9 OPTIONS wicked   --file mmap-para --iter 50000 --threads 5 --size 20 --alloc_init 1 --alloc_inc 1.2)
    TkrzwRunCheckFilePerf(INDEX  10 OPTIONS sequence --file mmap-atom --iter 50000 --threads 5 --size 20 --alloc_init 1 --alloc_inc 1.2)
    TkrzwRunCheckFilePerf(INDEX  11 OPTIONS sequence --file mmap-atom --iter 50000 --threads 5 --size 20 --alloc_init 1 --alloc_inc 1.2 --random)
    TkrzwRunCheckFilePerf(INDEX  12 OPTIONS sequence --file mmap-atom --iter 50000 --threads 5 --size 20 --alloc_init 1 --alloc_inc 1.2 --append)
    TkrzwRunCheckFilePerf(INDEX  13 OPTIONS wicked   --file mmap-atom --iter 50000 --threads 5 --size 20 --alloc_init 1 --alloc_inc 1.2)
    TkrzwRunCheckFilePerf(INDEX  14 OPTIONS sequence --file pos-para --iter 50000 --threads 5 --size 20 --alloc_init 1 --alloc_inc 1.2)
    TkrzwRunCheckFilePerf(INDEX  15 OPTIONS sequence --file pos-para --iter 50000 --threads 5 --size 20 --alloc_init 1 --alloc_inc 1.2 --random)
    TkrzwRunCheckFilePerf(INDEX  16 OPTIONS sequence --file pos-para --iter 50000 --threads 5 --size 20 --alloc_init 1 --alloc_inc 1.2 --append)
    TkrzwRunCheckFilePerf(INDEX  17 OPTIONS wicked   --file pos-para --iter 50000 --threads 5 --size 20 --alloc_init 1 --alloc_inc 1.2)
    TkrzwRunCheckFilePerf(INDEX  18 OPTIONS sequence --file pos-para --iter 5000 --threads 4 --size 512 --block_size 512 --direct_io --padding)
    TkrzwRunCheckFilePerf(INDEX  19 OPTIONS sequence --file pos-para --iter 5000 --threads 4 --size 512 --block_size 512 --direct_io --pagecache --padding)
    TkrzwRunCheckFilePerf(INDEX  20 OPTIONS sequence --file pos-para --iter 50000 --threads 5 --size 5 --block_size 32 --head_buffer 1000 --random)
    TkrzwRunCheckFilePerf(INDEX  21 OPTIONS sequence --file pos-para --iter 50000 --threads 5 --size 5 --block_size 32 --head_buffer 1000 --append)
    TkrzwRunCheckFilePerf(INDEX  22 OPTIONS wicked --file pos-para --iter 50000 --threads 5 --size 128 --block_size 128 --head_buffer 8192)
    TkrzwRunCheckFilePerf(INDEX  23 OPTIONS wicked --file pos-para --iter 50000 --threads 5 --size 128 --block_size 128 --pagecache --head_buffer 8192)
    TkrzwRunCheckFilePerf(INDEX  24 OPTIONS sequence --file pos-atom --iter 50000 --threads 5 --size 20 --alloc_init 1 --alloc_inc 1.2)
    TkrzwRunCheckFilePerf(INDEX  25 OPTIONS sequence --file pos-atom --iter 50000 --threads 5 --size 20 --alloc_init 1 --alloc_inc 1.2 --random)
    TkrzwRunCheckFilePerf(INDEX  26 OPTIONS sequence --file pos-atom --iter 50000 --threads 5 --size 20 --alloc_init 1 --alloc_inc 1.2 --append)
    TkrzwRunCheckFilePerf(INDEX  27 OPTIONS wicked --file pos-atom --iter 50000 --threads 5 --size 20 --alloc_init 1 --alloc_inc 1.2)
    TkrzwRunCheckFilePerf(INDEX  28 OPTIONS sequence --file pos-atom --iter 5000 --threads 4 --size 512 --block_size 512 --direct_io --padding)
    TkrzwRunCheckFilePerf(INDEX  29 OPTIONS sequence --file pos-atom --iter 5000 --threads 4 --size 512 --block_size 512 --direct_io --pagecache --padding)
    TkrzwRunCheckFilePerf(INDEX  30 OPTIONS sequence --file pos-atom --iter 50000 --threads 5 --size 5 --block_size 32 --head_buffer 1000 --random)
    TkrzwRunCheckFilePerf(INDEX  31 OPTIONS sequence --file pos-atom --iter 50000 --threads 5 --size 5 --block_size 32 --head_buffer 1000 --append)
    TkrzwRunCheckFilePerf(INDEX  32 OPTIONS wicked --file pos-atom --iter 50000 --threads 5 --size 128 --block_size 128 --head_buffer 8192)
    TkrzwRunCheckFilePerf(INDEX  33 OPTIONS wicked --file pos-atom --iter 50000 --threads 5 --size 128 --block_size 128 --pagecache --head_buffer 8192)

    function(TkrzwRunCheckHashDBMPerf)
        #add_test(NAME check-file-perf:1  COMMAND tkrzw_dbm_perf sequence --dbm hash --file mmap-para --iter 20000 --threads 5 --size 8 --buckets 100000 --path casket-somename.tkh)
        #              |- hardcoded -| ^          |--hardcoded-| |------------------------------------- OPTIONS ---------------------------------------|       |- generated name -| 
        #                              |                         
        #                            INDEX                       
        cmake_parse_arguments(params "" "INDEX" "OPTIONS" ${ARGN})
        add_test(
            NAME
                check-hashdbm-perf:${params_INDEX}
            COMMAND
                tkrzw_dbm_perf ${params_OPTIONS} --path casket-check-hashdbm-perf.${params_INDEX}.tkh
        )
    endfunction()

    TkrzwRunCheckHashDBMPerf(INDEX   1 OPTIONS sequence --dbm hash --file mmap-para --iter 20000 --threads 5 --size 8 --buckets 100000)
    TkrzwRunCheckHashDBMPerf(INDEX   2 OPTIONS sequence --dbm hash --file mmap-para --iter 20000 --threads 5 --size 8 --buckets 100000 --append)
    TkrzwRunCheckHashDBMPerf(INDEX   3 OPTIONS sequence --dbm hash --file mmap-para --iter 20000 --threads 5 --size 8 --buckets 100000 --random_key --random_value)
    TkrzwRunCheckHashDBMPerf(INDEX   4 OPTIONS sequence --dbm hash --file mmap-para --iter 20000 --threads 5 --size 8 --buckets 100000 --random_key --random_value --append --record_crc 8 --validate)
    TkrzwRunCheckHashDBMPerf(INDEX   5 OPTIONS parallel --dbm hash --file mmap-para --iter 20000 --threads 5 --size 8 --buckets 100000 --random_key --random_value)
    TkrzwRunCheckHashDBMPerf(INDEX   6 OPTIONS parallel --dbm hash --file mmap-para --iter 20000 --threads 5 --size 8 --buckets 100000 --random_key --random_value --append)
    TkrzwRunCheckHashDBMPerf(INDEX   7 OPTIONS parallel --dbm hash --file mmap-para --iter 100000 --threads 1 --size 8 --buckets 10000 --random_key --random_value --rebuild)
    TkrzwRunCheckHashDBMPerf(INDEX   8 OPTIONS parallel --dbm hash --file mmap-para --iter 100000 --threads 1 --size 8 --buckets 10000 --random_key --random_value --keys 10000 --rebuild --append --record_crc 8 --validate)
    TkrzwRunCheckHashDBMPerf(INDEX   9 OPTIONS parallel --dbm hash --file mmap-para --iter 10000 --threads 5 --size 8 --buckets 1000 --random_key --random_value --keys 5000 --rebuild --append  --record_crc 16 --sleep 0.00001)
    TkrzwRunCheckHashDBMPerf(INDEX  10 OPTIONS wicked --dbm hash --file mmap-para --iter 20000 --threads 5 --size 8 --buckets 100000 --iterator --clear --rebuild)
    TkrzwRunCheckHashDBMPerf(INDEX  11 OPTIONS wicked --dbm hash --file mmap-para --iter 20000 --threads 5 --size 8 --buckets 100000 --append --record_crc 32 --iterator --clear --rebuild)
    TkrzwRunCheckHashDBMPerf(INDEX  12 OPTIONS wicked --dbm hash --file pos-para --block_size 512 --direct_io --padding --iter 5000 --threads 1 --size 100 --buckets 100000 --iterator --clear --rebuild)
    TkrzwRunCheckHashDBMPerf(INDEX  13 OPTIONS wicked --dbm hash --file pos-atom --block_size 512 --direct_io --padding --iter 5000 --threads 2 --size 100 --buckets 100000 --iterator --clear --rebuild)
    TkrzwRunCheckHashDBMPerf(INDEX  14 OPTIONS wicked --dbm hash --file pos-atom --block_size 512 --direct_io --padding --pagecache --iter 10000 --threads 4 --size 100 --buckets 100000 --iterator --clear --rebuild --validate)
    TkrzwRunCheckHashDBMPerf(INDEX  15 OPTIONS wicked --record_comp rc4 --cipher_key abc --iter 10000 --threads 4 --size 100 --buckets 100000 --iterator --clear --rebuild --validate)

    add_test(NAME check-hashdbm-util:setup COMMAND ${CMAKE_COMMAND} -E rm -rf casket-check-hashdbm-util*)
    add_test(NAME check-hashdbm-util:1 COMMAND tkrzw_dbm_util create --dbm hash --record_crc 8 --buckets 10 casket-check-hashdbm-util.tkh)
    add_test(NAME check-hashdbm-util:2 COMMAND tkrzw_dbm_util  set --dbm hash casket-check-hashdbm-util.tkh one first)
    add_test(NAME check-hashdbm-util:3 COMMAND tkrzw_dbm_util  set --dbm hash casket-check-hashdbm-util.tkh two second)
    add_test(NAME check-hashdbm-util:4 COMMAND tkrzw_dbm_util  set --dbm hash casket-check-hashdbm-util.tkh three third)
    add_test(NAME check-hashdbm-util:5 COMMAND tkrzw_dbm_util  set --dbm hash casket-check-hashdbm-util.tkh four fourth)
    add_test(NAME check-hashdbm-util:6 COMMAND tkrzw_dbm_util  remove --dbm hash casket-check-hashdbm-util.tkh four)
    add_test(NAME check-hashdbm-util:7 COMMAND tkrzw_dbm_util  set --dbm hash casket-check-hashdbm-util.tkh five fifth)
    add_test(NAME check-hashdbm-util:8 COMMAND tkrzw_dbm_util  get --dbm hash casket-check-hashdbm-util.tkh two)
    add_test(NAME check-hashdbm-util:9 COMMAND tkrzw_dbm_util  rekey --dbm hash --no_overwrite casket-check-hashdbm-util.tkh five FIVE)
    add_test(NAME check-hashdbm-util:10 COMMAND tkrzw_dbm_util  get --dbm hash casket-check-hashdbm-util.tkh FIVE)
    add_test(NAME check-hashdbm-util:11 COMMAND tkrzw_dbm_util  set --dbm hash --multi casket-check-hashdbm-util.tkh 1 ichi 2 ni)
    add_test(NAME check-hashdbm-util:12 COMMAND tkrzw_dbm_util  set --dbm hash --multi --append ":" casket-check-hashdbm-util.tkh 1 a 2 b)
    add_test(NAME check-hashdbm-util:13 COMMAND tkrzw_dbm_util  get --dbm hash --multi casket-check-hashdbm-util.tkh 1 2 3)
    add_test(NAME check-hashdbm-util:14 COMMAND tkrzw_dbm_util  remove --dbm hash --multi casket-check-hashdbm-util.tkh 1 2 3)
    add_test(NAME check-hashdbm-util:15 COMMAND tkrzw_dbm_util  export --dbm hash casket-check-hashdbm-util.tkh casket-check-hashdbm-util.flat)
    add_test(NAME check-hashdbm-util:16 COMMAND tkrzw_dbm_util  create --dbm hash --record_crc 8 --buckets 10 casket-check-hashdbm-util-2.tkh)
    add_test(NAME check-hashdbm-util:17 COMMAND tkrzw_dbm_util  import --dbm hash casket-check-hashdbm-util-2.tkh casket-check-hashdbm-util.flat)
    add_test(NAME check-hashdbm-util:18 COMMAND tkrzw_dbm_util  list --dbm hash casket-check-hashdbm-util-2.tkh)
    add_test(NAME check-hashdbm-util:19 COMMAND tkrzw_dbm_util  list --dbm hash --move jump --jump_key three --items 2 casket-check-hashdbm-util-2.tkh)
    add_test(NAME check-hashdbm-util:20 COMMAND tkrzw_dbm_util  export --dbm hash --tsv casket-check-hashdbm-util.tkh casket-check-hashdbm-util.tsv)
    add_test(NAME check-hashdbm-util:21 COMMAND tkrzw_dbm_util  create --dbm hash --record_crc 8 --buckets 10 casket-check-hashdbm-util-3.tkh)
    add_test(NAME check-hashdbm-util:22 COMMAND tkrzw_dbm_util  import --dbm hash --tsv casket-check-hashdbm-util-3.tkh casket-check-hashdbm-util.tsv)
    add_test(NAME check-hashdbm-util:23 COMMAND tkrzw_dbm_util  get --dbm hash casket-check-hashdbm-util-3.tkh three)
    add_test(NAME check-hashdbm-util:24 COMMAND tkrzw_dbm_util  export --dbm hash --ulog 123 casket-check-hashdbm-util.tkh casket-check-hashdbm-util-ulog)
    add_test(NAME check-hashdbm-util:25 COMMAND tkrzw_dbm_util  import --dbm hash --ulog 123 casket-check-hashdbm-util-4.tkh casket-check-hashdbm-util-ulog)
    add_test(NAME check-hashdbm-util:26 COMMAND tkrzw_dbm_util  get --dbm hash casket-check-hashdbm-util-4.tkh three)
    add_test(NAME check-hashdbm-util:27 COMMAND tkrzw_dbm_util  rebuild --dbm hash casket-check-hashdbm-util.tkh)
    add_test(NAME check-hashdbm-util:28 COMMAND tkrzw_dbm_util  inspect --dbm hash --validate casket-check-hashdbm-util.tkh)
    add_test(NAME check-hashdbm-util:29 COMMAND tkrzw_dbm_util  restore --dbm hash casket-check-hashdbm-util.tkh casket-check-hashdbm-util-new.tkh)
    add_test(NAME check-hashdbm-util:30 COMMAND tkrzw_dbm_util  set --dbm hash casket-check-hashdbm-util-new.tkh four fourth)
    add_test(NAME check-hashdbm-util:31 COMMAND tkrzw_dbm_util  get --dbm hash casket-check-hashdbm-util-new.tkh one)
    add_test(NAME check-hashdbm-util:32 COMMAND tkrzw_dbm_util  list --dbm hash casket-check-hashdbm-util-new.tkh)
    add_test(NAME check-hashdbm-util:33 COMMAND tkrzw_dbm_util  inspect --dbm hash --validate casket-check-hashdbm-util-new.tkh)
    add_test(NAME check-hashdbm-util:34 COMMAND tkrzw_dbm_util  merge --dbm hash casket-check-hashdbm-util-new.tkh casket-check-hashdbm-util.tkh)
    add_test(NAME check-hashdbm-util:35 COMMAND tkrzw_dbm_util  create --dbm hash --file pos-para --alloc_init 1 --alloc_inc 1.2 --buckets 10 --block_size 8192 --padding --truncate casket-check-hashdbm-util.tkh)
    add_test(NAME check-hashdbm-util:36 COMMAND tkrzw_dbm_util  set --dbm hash --file pos-para --no_wait --alloc_init 1 --alloc_inc 1.2 --block_size 8192 --padding casket-check-hashdbm-util.tkh tokyo shinjuku)
    add_test(NAME check-hashdbm-util:37 COMMAND tkrzw_dbm_util  rebuild --dbm hash --file pos-para --no_lock --alloc_init 1 --alloc_inc 1.2 --block_size 8192 --padding casket-check-hashdbm-util.tkh)
    add_test(NAME check-hashdbm-util:38 COMMAND tkrzw_dbm_util  set --dbm hash --file pos-para --block_size 8192 --padding casket-check-hashdbm-util.tkh saitama tokorozawa)
    add_test(NAME check-hashdbm-util:39 COMMAND tkrzw_dbm_util  set --dbm hash --file pos-para --block_size 8192 --padding casket-check-hashdbm-util.tkh kanagawa yokohama)
    add_test(NAME check-hashdbm-util:40 COMMAND tkrzw_dbm_util  get --dbm hash --file pos-para --block_size 8192 --padding casket-check-hashdbm-util.tkh saitama)
    add_test(NAME check-hashdbm-util:41 COMMAND tkrzw_dbm_util  remove --dbm hash --file pos-para --block_size 8192 --padding casket-check-hashdbm-util.tkh saitama)
    add_test(NAME check-hashdbm-util:42 COMMAND tkrzw_dbm_util  list --dbm hash --file pos-para --no_lock --alloc_init 1 --alloc_inc 1.2 --block_size 8192 --padding casket-check-hashdbm-util.tkh)
    add_test(NAME check-hashdbm-util:43 COMMAND tkrzw_dbm_util  create --record_comp rc4 --cipher_key abc --buckets 10 casket-check-hashdbm-util-rc4.tkh)
    add_test(NAME check-hashdbm-util:44 COMMAND tkrzw_dbm_util  set --cipher_key abc casket-check-hashdbm-util-rc4.tkh one first)
    add_test(NAME check-hashdbm-util:45 COMMAND tkrzw_dbm_util  set --cipher_key abc casket-check-hashdbm-util-rc4.tkh two second)
    add_test(NAME check-hashdbm-util:46 COMMAND tkrzw_dbm_util  get --cipher_key abc casket-check-hashdbm-util-rc4.tkh one)
    add_test(NAME check-hashdbm-util:47 COMMAND tkrzw_dbm_util  rebuild --cipher_key abc casket-check-hashdbm-util-rc4.tkh)
    add_test(NAME check-hashdbm-util:48 COMMAND tkrzw_dbm_util  get --cipher_key abc casket-check-hashdbm-util-rc4.tkh two)
    add_test(NAME check-hashdbm-util:49 COMMAND tkrzw_dbm_util  restore --cipher_key abc casket-check-hashdbm-util-rc4.tkh casket-check-hashdbm-util-rc4-new.tkh)
    add_test(NAME check-hashdbm-util:50 COMMAND tkrzw_dbm_util  get --cipher_key abc casket-check-hashdbm-util-rc4-new.tkh one)
    add_test(NAME check-hashdbm-util:51 COMMAND tkrzw_dbm_util  set --cipher_key abc casket-check-hashdbm-util-rc4-new.tkh three third)
    add_test(NAME check-hashdbm-util:52 COMMAND tkrzw_dbm_util  merge --cipher_key abc casket-check-hashdbm-util-rc4.tkh casket-check-hashdbm-util-rc4-new.tkh)
    add_test(NAME check-hashdbm-util:53 COMMAND tkrzw_dbm_util  get --cipher_key abc casket-check-hashdbm-util-rc4.tkh three)
    add_test(NAME check-hashdbm-util:54 COMMAND tkrzw_dbm_util  remove --cipher_key abc casket-check-hashdbm-util-rc4.tkh two)
    add_test(NAME check-hashdbm-util:55 COMMAND tkrzw_dbm_util  list --cipher_key abc casket-check-hashdbm-util-rc4.tkh)


    set_tests_properties(check-hashdbm-util:1 PROPERTIES DEPENDS check-hashdbm-util:setup)
    foreach(index RANGE 2 55)
        math(EXPR previous_index "${index}-1")
        set_tests_properties(check-hashdbm-util:${index} PROPERTIES DEPENDS check-hashdbm-util:${previous_index})
    endforeach()

endif (TKRZW_TESTS)

