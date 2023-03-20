find_package(CapnProto)
find_package(cppzmq)

capnp_generate_cpp(CAPNP_SRCS CAPNP_HDRS SNinterface.capnp)

add_library(rpc STATIC ${CAPNP_SRCS})
target_link_libraries(rpc PRIVATE CapnProto::capnp-rpc)
target_link_libraries(rpc PRIVATE CapnProto::capnp)
target_link_libraries(rpc PRIVATE cppzmq)
target_link_libraries(rpc PUBLIC jemalloc)
target_include_directories(rpc PRIVATE ${PROJECT_BINARY_DIR}/rpc)