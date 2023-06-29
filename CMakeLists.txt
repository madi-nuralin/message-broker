cmake_minimum_required(VERSION 3.16)

project(client-rbmq VERSION 1.0)

find_package(PkgConfig REQUIRED)
pkg_check_modules(RBMQ REQUIRED librabbitmq>=0.10.0)
pkg_check_modules(GLIB REQUIRED glib-2.0>=0.29.2)

include_directories(${RBMQ_INCLUDE_DIRS} ${GLIB_INCLUDE_DIRS} ${CMAKE_CURRENT_BINARY_DIR})
link_directories(${RBMQ_LIBRARY_DIRS} ${GLIB_LIBRARY_DIRS})
link_libraries(${RBMQ_LIBRARIES} ${GLIB_LIBRARIES})

#configure_file(client_rbmq_config.h.in client_rbmq_config.h @ONLY)

add_executable(client_rpc client_rpc.cpp notification.cpp utils.cpp)
add_executable(server_rpc server_rpc.cpp notification.cpp utils.cpp)
