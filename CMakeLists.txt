cmake_minimum_required(VERSION 3.16)

project(client-rbmq VERSION 1.0)

find_package(PkgConfig REQUIRED)

add_executable(client client.cpp)
add_executable(server server.cpp)

target_link_libraries(client amqpcpp pthread dl ev event)
target_link_libraries(server amqpcpp pthread dl ev event)
