cmake_minimum_required(VERSION 3.16)
project(kv_store CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# ── Release build with full native optimisations ──────────────────────────────
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()
add_compile_options(
  -O3 -march=native -mtune=native
  -funroll-loops -fomit-frame-pointer
  -Wall -Wextra -Wno-unused-parameter
)
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static-libstdc++ -static-libgcc")

# ── Find packages ─────────────────────────────────────────────────────────────
find_package(Threads REQUIRED)

# gRPC / Protobuf — use cmake configs from /usr/local (grpc v1.58 + protobuf v23)
# Protobuf MUST be found before gRPC (gRPC config checks protobuf:: targets at load time)
set(CMAKE_PREFIX_PATH "/usr/local" ${CMAKE_PREFIX_PATH})
find_package(absl CONFIG QUIET)
find_package(protobuf CONFIG REQUIRED HINTS /usr/local/lib/cmake/protobuf)
find_package(gRPC CONFIG REQUIRED HINTS /usr/local/lib/cmake/grpc)

# RocksDB
find_package(RocksDB CONFIG QUIET)
if(NOT RocksDB_FOUND)
  find_library(ROCKSDB_LIB NAMES rocksdb PATHS /usr/lib /usr/lib/x86_64-linux-gnu REQUIRED)
  find_path(ROCKSDB_INCLUDE_DIR rocksdb/db.h PATHS /usr/include REQUIRED)
  set(ROCKSDB_LIBRARIES ${ROCKSDB_LIB})
  set(ROCKSDB_INCLUDE_DIRS ${ROCKSDB_INCLUDE_DIR})
endif()

# jwt-cpp — use bundled directory (has both jwt-cpp and picojson headers)
set(JWT_INCLUDE_DIR ${CMAKE_SOURCE_DIR}/jwt-cpp/include)
if(NOT EXISTS "${JWT_INCLUDE_DIR}/jwt-cpp/jwt.h")
  find_path(JWT_INCLUDE_DIR jwt-cpp/jwt.h REQUIRED)
endif()
message(STATUS "jwt-cpp found at: ${JWT_INCLUDE_DIR}")

# ── Proto codegen ─────────────────────────────────────────────────────────────
set(PROTO_DIR   ${CMAKE_SOURCE_DIR}/proto)
set(PROTO_OUT   ${CMAKE_BINARY_DIR}/proto_gen)
file(MAKE_DIRECTORY ${PROTO_OUT})

# Use protoc and plugin from the gRPC cmake config
get_target_property(GRPC_CPP_PLUGIN gRPC::grpc_cpp_plugin IMPORTED_LOCATION_NOCONFIG)
if(NOT GRPC_CPP_PLUGIN)
  get_target_property(GRPC_CPP_PLUGIN gRPC::grpc_cpp_plugin IMPORTED_LOCATION)
endif()
if(NOT GRPC_CPP_PLUGIN)
  find_program(GRPC_CPP_PLUGIN NAMES grpc_cpp_plugin HINTS /usr/local/bin REQUIRED)
endif()
find_program(PROTOC_EXE protoc HINTS /usr/local/bin NO_DEFAULT_PATH)
if(NOT PROTOC_EXE)
  find_program(PROTOC_EXE protoc)
endif()
message(STATUS "protoc: ${PROTOC_EXE}")
message(STATUS "grpc_cpp_plugin: ${GRPC_CPP_PLUGIN}")

add_custom_command(
  OUTPUT  ${PROTO_OUT}/kv.pb.cc ${PROTO_OUT}/kv.pb.h
          ${PROTO_OUT}/kv.grpc.pb.cc ${PROTO_OUT}/kv.grpc.pb.h
  COMMAND ${PROTOC_EXE}
    --proto_path=${PROTO_DIR}
    --cpp_out=${PROTO_OUT}
    --grpc_out=${PROTO_OUT}
    --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
    ${PROTO_DIR}/kv.proto
  DEPENDS ${PROTO_DIR}/kv.proto
)

add_library(kv_proto STATIC
  ${PROTO_OUT}/kv.pb.cc
  ${PROTO_OUT}/kv.grpc.pb.cc
)
target_include_directories(kv_proto PUBLIC ${PROTO_OUT})
target_link_libraries(kv_proto PUBLIC gRPC::grpc++ protobuf::libprotobuf)

# ── Helper: link everything a binary needs ────────────────────────────────────
function(kv_binary TARGET SRC)
  add_executable(${TARGET} ${SRC})
  target_include_directories(${TARGET} PRIVATE ${JWT_INCLUDE_DIR} ${PROTO_OUT})
  target_link_libraries(${TARGET} PRIVATE kv_proto Threads::Threads
    gRPC::grpc++ protobuf::libprotobuf)
endfunction()

# ── Targets ───────────────────────────────────────────────────────────────────
kv_binary(storage_node  src/storage_node.cpp)
kv_binary(api_node      src/api_node.cpp)
kv_binary(load_client   src/load_client.cpp)

# RocksDB only needed by storage_node
if(RocksDB_FOUND)
  target_link_libraries(storage_node PRIVATE RocksDB::rocksdb)
else()
  target_link_libraries(storage_node PRIVATE ${ROCKSDB_LIBRARIES})
  target_include_directories(storage_node PRIVATE ${ROCKSDB_INCLUDE_DIRS})
endif()

# Diagnostic test storage client
# kv_binary(test_storage  /tmp/test_storage.cpp)