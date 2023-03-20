OPTION(EMULATE_AURORA "compile with aurora emulation system" OFF) # Disabled by default

IF(EMULATE_AURORA)
    ADD_DEFINITIONS(-DEMULATE_AURORA)
    message("Using OCC")
ELSE()
    message("Using transaction repair")
ENDIF(EMULATE_AURORA)

add_subdirectory(rpc) 

# set header files
include_directories(".")

file(GLOB SOURCES
    "main.cpp" 
    "txn/*.cpp"
    )

add_executable(hackwrench_fast_to "${CMAKE_CURRENT_SOURCE_DIR}/main.cpp")
target_include_directories(hackwrench_fast_to PRIVATE ${PROJECT_BINARY_DIR}/rpc)
target_compile_options(hackwrench_fast_to PRIVATE "-DFINE_VALIDATION" "-DSN_BROADCAST" "-DWORKLOAD_FAST_PATH=true" "-DTOTAL_ORDER_TS")
target_link_libraries(hackwrench_fast_to PRIVATE rpc)

add_executable(hackwrench_normal_to "${CMAKE_CURRENT_SOURCE_DIR}/main.cpp")
target_include_directories(hackwrench_normal_to PRIVATE ${PROJECT_BINARY_DIR}/rpc)
target_compile_options(hackwrench_normal_to PRIVATE "-DFINE_VALIDATION" "-DSN_BROADCAST" "-DNORMAL_PATH_OPT" "-DTOTAL_ORDER_TS")
target_link_libraries(hackwrench_normal_to PRIVATE rpc)
        
add_executable(hackwrench_to "${CMAKE_CURRENT_SOURCE_DIR}/main.cpp")
target_include_directories(hackwrench_to PRIVATE ${PROJECT_BINARY_DIR}/rpc)
target_compile_options(hackwrench_to PRIVATE "-DFINE_VALIDATION" "-DSN_BROADCAST" "-DTOTAL_ORDER_TS")
target_link_libraries(hackwrench_to PRIVATE rpc)

add_executable(hackwrench "${CMAKE_CURRENT_SOURCE_DIR}/main.cpp")
target_include_directories(hackwrench PRIVATE ${PROJECT_BINARY_DIR}/rpc)
target_compile_options(hackwrench PRIVATE "-DFINE_VALIDATION" "-DSN_BROADCAST")
target_link_libraries(hackwrench PRIVATE rpc)

add_executable(hackwrench_non_caching "${CMAKE_CURRENT_SOURCE_DIR}/main.cpp")
target_include_directories(hackwrench_non_caching PRIVATE ${PROJECT_BINARY_DIR}/rpc)
target_compile_options(hackwrench_non_caching PRIVATE "-DFINE_VALIDATION" "-DSN_BROADCAST" "-DNON_CACHING")
target_link_libraries(hackwrench_non_caching PRIVATE rpc)

add_executable(hackwrench_normal "${CMAKE_CURRENT_SOURCE_DIR}/main.cpp")
target_include_directories(hackwrench_normal PRIVATE ${PROJECT_BINARY_DIR}/rpc)
target_compile_options(hackwrench_normal PRIVATE "-DFINE_VALIDATION" "-DSN_BROADCAST" "-DNORMAL_PATH_OPT")
target_link_libraries(hackwrench_normal PRIVATE rpc)

add_executable(hackwrench_normal_non_caching "${CMAKE_CURRENT_SOURCE_DIR}/main.cpp")
target_include_directories(hackwrench_normal_non_caching PRIVATE ${PROJECT_BINARY_DIR}/rpc)
target_compile_options(hackwrench_normal_non_caching PRIVATE "-DFINE_VALIDATION" "-DSN_BROADCAST" "-DNORMAL_PATH_OPT" "-DNON_CACHING")
target_link_libraries(hackwrench_normal_non_caching PRIVATE rpc)

add_executable(hackwrench_fast "${CMAKE_CURRENT_SOURCE_DIR}/main.cpp")
target_include_directories(hackwrench_fast PRIVATE ${PROJECT_BINARY_DIR}/rpc)
target_compile_options(hackwrench_fast PRIVATE "-DFINE_VALIDATION" "-DSN_BROADCAST" "-DWORKLOAD_FAST_PATH=true")
target_link_libraries(hackwrench_fast PRIVATE rpc)

add_executable(hackwrench_fast_non_caching "${CMAKE_CURRENT_SOURCE_DIR}/main.cpp")
target_include_directories(hackwrench_fast_non_caching PRIVATE ${PROJECT_BINARY_DIR}/rpc)
target_compile_options(hackwrench_fast_non_caching PRIVATE "-DFINE_VALIDATION" "-DSN_BROADCAST" "-DWORKLOAD_FAST_PATH=true" "-DNON_CACHING")
target_link_libraries(hackwrench_fast_non_caching PRIVATE rpc)

add_executable(hackwrench_lat "${CMAKE_CURRENT_SOURCE_DIR}/main.cpp")
target_include_directories(hackwrench_lat PRIVATE ${PROJECT_BINARY_DIR}/rpc)
target_compile_options(hackwrench_lat PRIVATE "-DFINE_VALIDATION" "-DSN_BROADCAST" "-DDETAILED_TXN_LAT")
target_link_libraries(hackwrench_lat PRIVATE rpc)


add_executable(hackwrench_normal_lat "${CMAKE_CURRENT_SOURCE_DIR}/main.cpp")
target_include_directories(hackwrench_normal_lat PRIVATE ${PROJECT_BINARY_DIR}/rpc)
target_compile_options(hackwrench_normal_lat PRIVATE "-DFINE_VALIDATION" "-DSN_BROADCAST" "-DNORMAL_PATH_OPT" "-DDETAILED_TXN_LAT")
target_link_libraries(hackwrench_normal_lat PRIVATE rpc)

add_executable(hackwrench_fast_lat "${CMAKE_CURRENT_SOURCE_DIR}/main.cpp")
target_include_directories(hackwrench_fast_lat PRIVATE ${PROJECT_BINARY_DIR}/rpc)
target_compile_options(hackwrench_fast_lat PRIVATE "-DFINE_VALIDATION" "-DWORKLOAD_FAST_PATH=true" "-DDETAILED_TXN_LAT")
target_link_libraries(hackwrench_fast_lat PRIVATE rpc)

add_executable(hackwrench_coco "${CMAKE_CURRENT_SOURCE_DIR}/main.cpp")
target_include_directories(hackwrench_coco PRIVATE ${PROJECT_BINARY_DIR}/rpc)
target_compile_options(hackwrench_coco PRIVATE "-DCOCO" "-DSN_BROADCAST")
target_link_libraries(hackwrench_coco PRIVATE rpc)

add_executable(hackwrench_coco_fast "${CMAKE_CURRENT_SOURCE_DIR}/main.cpp")
target_include_directories(hackwrench_coco_fast PRIVATE ${PROJECT_BINARY_DIR}/rpc)
target_compile_options(hackwrench_coco_fast PRIVATE "-DCOCO" "-DSN_BROADCAST" "-DWORKLOAD_FAST_PATH=true")
target_link_libraries(hackwrench_coco_fast PRIVATE rpc)

add_executable(hackwrench_coco_normal "${CMAKE_CURRENT_SOURCE_DIR}/main.cpp")
target_include_directories(hackwrench_coco_normal PRIVATE ${PROJECT_BINARY_DIR}/rpc)
target_compile_options(hackwrench_coco_normal PRIVATE "-DCOCO" "-DSN_BROADCAST" "-DNORMAL_PATH_OPT")
target_link_libraries(hackwrench_coco_normal PRIVATE rpc)

add_executable(hackwrench_pure_occ_non_caching "${CMAKE_CURRENT_SOURCE_DIR}/main.cpp")
target_include_directories(hackwrench_pure_occ_non_caching PRIVATE ${PROJECT_BINARY_DIR}/rpc)
target_compile_options(hackwrench_pure_occ_non_caching PRIVATE "-DEMULATE_AURORA" "-DFINE_AURORA" "-DNON_CACHING" "-DPURE_OCC")
target_link_libraries(hackwrench_pure_occ_non_caching PRIVATE rpc)

add_executable(hackwrench_occ_non_caching "${CMAKE_CURRENT_SOURCE_DIR}/main.cpp")
target_include_directories(hackwrench_occ_non_caching PRIVATE ${PROJECT_BINARY_DIR}/rpc)
target_compile_options(hackwrench_occ_non_caching PRIVATE "-DEMULATE_AURORA" "-DFINE_AURORA" "-DNON_CACHING")
target_link_libraries(hackwrench_occ_non_caching PRIVATE rpc)

add_executable(hackwrench_occ_non_caching_lat "${CMAKE_CURRENT_SOURCE_DIR}/main.cpp")
target_include_directories(hackwrench_occ_non_caching_lat PRIVATE ${PROJECT_BINARY_DIR}/rpc)
target_compile_options(hackwrench_occ_non_caching_lat PRIVATE "-DEMULATE_AURORA" "-DFINE_AURORA" "-DNON_CACHING" "-DDETAILED_TXN_LAT")
target_link_libraries(hackwrench_occ_non_caching_lat PRIVATE rpc)

add_executable(hackwrench_pure_occ "${CMAKE_CURRENT_SOURCE_DIR}/main.cpp")
target_include_directories(hackwrench_pure_occ PRIVATE ${PROJECT_BINARY_DIR}/rpc)
target_compile_options(hackwrench_pure_occ PRIVATE "-DEMULATE_AURORA" "-DFINE_AURORA" "-DPURE_OCC")
target_link_libraries(hackwrench_pure_occ PRIVATE rpc)

add_executable(hackwrench_occ "${CMAKE_CURRENT_SOURCE_DIR}/main.cpp")
target_include_directories(hackwrench_occ PRIVATE ${PROJECT_BINARY_DIR}/rpc)
target_compile_options(hackwrench_occ PRIVATE "-DEMULATE_AURORA" "-DFINE_AURORA")
target_link_libraries(hackwrench_occ PRIVATE rpc)

add_executable(hackwrench_occ_lat "${CMAKE_CURRENT_SOURCE_DIR}/main.cpp")
target_include_directories(hackwrench_occ_lat PRIVATE ${PROJECT_BINARY_DIR}/rpc)
target_compile_options(hackwrench_occ_lat PRIVATE "-DEMULATE_AURORA" "-DDETAILED_TXN_LAT" "-DFINE_AURORA")
target_link_libraries(hackwrench_occ_lat PRIVATE rpc)

add_executable(hackwrench_batch_abort "${CMAKE_CURRENT_SOURCE_DIR}/main.cpp")
target_include_directories(hackwrench_batch_abort PRIVATE ${PROJECT_BINARY_DIR}/rpc)
target_compile_options(hackwrench_batch_abort PRIVATE "-DNO_TS" "-DABORT_ON_FAIL" "-DFINE_AURORA" "-DSN_BROADCAST")
target_link_libraries(hackwrench_batch_abort PRIVATE rpc)

add_executable(hackwrench_batch_abort_committed "${CMAKE_CURRENT_SOURCE_DIR}/main.cpp")
target_include_directories(hackwrench_batch_abort_committed PRIVATE ${PROJECT_BINARY_DIR}/rpc)
target_compile_options(hackwrench_batch_abort_committed PRIVATE "-DNO_TS" "-DABORT_ON_FAIL" "-DFINE_AURORA" "-DSN_BROADCAST" "-DREAD_COMMITTED")
target_link_libraries(hackwrench_batch_abort_committed PRIVATE rpc)

add_executable(hackwrench_batch_abort_0 "${CMAKE_CURRENT_SOURCE_DIR}/main.cpp")
target_include_directories(hackwrench_batch_abort_0 PRIVATE ${PROJECT_BINARY_DIR}/rpc)
target_compile_options(hackwrench_batch_abort_0 PRIVATE "-DNO_TS" "-DABORT_ON_FAIL" "-DFINE_VALIDATION" "-DSN_BROADCAST" "-DDETERMINISTIC_VALIDATION" "-DNO_CONTENTION")
target_link_libraries(hackwrench_batch_abort_0 PRIVATE rpc)

add_executable(hackwrench_batch_abort_non_caching "${CMAKE_CURRENT_SOURCE_DIR}/main.cpp")
target_include_directories(hackwrench_batch_abort_non_caching PRIVATE ${PROJECT_BINARY_DIR}/rpc)
target_compile_options(hackwrench_batch_abort_non_caching PRIVATE "-DNO_TS" "-DABORT_ON_FAIL" "-DFINE_AURORA" "-DNON_CACHING")
target_link_libraries(hackwrench_batch_abort_non_caching PRIVATE rpc)

add_executable(hackwrench_ts "${CMAKE_CURRENT_SOURCE_DIR}/main.cpp")
target_include_directories(hackwrench_ts PRIVATE ${PROJECT_BINARY_DIR}/rpc)
target_compile_options(hackwrench_ts PRIVATE "-DABORT_ON_FAIL" "-DFINE_VALIDATION" "-DSN_BROADCAST")
target_link_libraries(hackwrench_ts PRIVATE rpc)

add_executable(hackwrench_ts_bottleneck "${CMAKE_CURRENT_SOURCE_DIR}/main.cpp")
target_include_directories(hackwrench_ts_bottleneck PRIVATE ${PROJECT_BINARY_DIR}/rpc)
target_compile_options(hackwrench_ts_bottleneck PRIVATE "-DABORT_ON_FAIL" "-DFINE_VALIDATION" "-DSN_BROADCAST" "-DTEST_TS" "-DTEST_TS_KEYGEN")
target_link_libraries(hackwrench_ts_bottleneck PRIVATE rpc)