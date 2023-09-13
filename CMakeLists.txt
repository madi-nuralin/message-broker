cmake_minimum_required(VERSION 3.16)

project(client-rbmq VERSION 1.0)

find_package(PkgConfig REQUIRED)
pkg_check_modules(RBMQ REQUIRED librabbitmq>=0.10.0)
pkg_check_modules(GLIB REQUIRED glib-2.0>=0.29.2)
pkg_check_modules(JSON_GLIB REQUIRED json-glib-1.0)

include_directories(${RBMQ_INCLUDE_DIRS} ${GLIB_INCLUDE_DIRS} ${JSON_GLIB_INCLUDE_DIRS} ${CMAKE_CURRENT_BINARY_DIR})
link_directories(${RBMQ_LIBRARY_DIRS} ${GLIB_LIBRARY_DIRS} ${JSON_GLIB_LIBRARY_DIRS})
link_libraries(${RBMQ_LIBRARIES} ${GLIB_LIBRARIES} ${JSON_GLIB_LIBRARIES})

#configure_file(client_rbmq_config.h.in client_rbmq_config.h @ONLY)

add_executable(client examples/client_main.cpp message_broker.cpp utils.cpp thread_pool.cpp)
add_executable(server examples/server_main.cpp message_broker.cpp utils.cpp thread_pool.cpp)
add_executable(server_rpc examples/server_rpc.cpp utils.cpp)
add_executable(client_rpc examples/client_rpc.cpp utils.cpp)
