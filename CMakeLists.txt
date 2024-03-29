cmake_minimum_required(VERSION 3.16)

project(message-broker VERSION 1.0)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread -DPTHREAD -m64 -fPIC -std=c++17 -ggdb -Wall -Wextra")

find_package(PkgConfig REQUIRED)
pkg_check_modules(RBMQ REQUIRED librabbitmq>=0.13.0)
pkg_check_modules(GLIB REQUIRED glib-2.0>=0.29.2)
#pkg_check_modules(JSON_GLIB REQUIRED json-glib-1.0)

include_directories(${RBMQ_INCLUDE_DIRS} ${GLIB_INCLUDE_DIRS} ${JSON_GLIB_INCLUDE_DIRS} ${CMAKE_CURRENT_BINARY_DIR})
link_directories(${RBMQ_LIBRARY_DIRS} ${GLIB_LIBRARY_DIRS} ${JSON_GLIB_LIBRARY_DIRS})
link_libraries(${RBMQ_LIBRARIES} ${GLIB_LIBRARIES} ${JSON_GLIB_LIBRARIES})

#configure_file(client_rbmq_config.h.in client_rbmq_config.h @ONLY)

add_executable(publisher examples/publisher.cpp message_broker.cpp utils.cpp)
add_executable(subscriber examples/subscriber.cpp message_broker.cpp utils.cpp)
add_executable(rpc_publisher examples/rpc_publisher.cpp message_broker.cpp utils.cpp)
add_executable(rpc_subscriber examples/rpc_subscriber.cpp message_broker.cpp utils.cpp)
add_executable(amqp_rpc_sendstring_server examples/amqp_rpc_sendstring_server.cpp utils.cpp)
add_executable(amqp_rpc_sendstring_client examples/amqp_rpc_sendstring_client.cpp utils.cpp)
