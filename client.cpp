#include <iostream>
#include <algorithm>
#include <thread>
#include <sstream>
#include <chrono>
#include <amqpcpp.h>
#include <amqpcpp/libev.h>

int main(int argc, char* argv[])
{
    std::stringstream ss;
    ss << std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count();
    std::string correlation = ss.str();

    auto *loop = EV_DEFAULT;

    AMQP::LibEvHandler handler(loop);

    AMQP::TcpConnection connection(&handler, AMQP::Address("localhost", 5672, AMQP::Login("guest", "guest"), "/"));

    AMQP::TcpChannel channel(&connection);

    channel.onError([](const char* message) {
        std::cerr << "Channel error: " << message << std::endl;
    });

    channel.declareQueue(AMQP::exclusive)
        .onSuccess([&channel, correlation](const std::string& name, int, int) {
            AMQP::Envelope env("3");
            env.setCorrelationID(correlation);
            env.setReplyTo(name);
            channel.publish("", "rpc_queue", env);
            std::cout << "Requesting fib(3)" << std::endl;
        }
    );

    channel.consume("", AMQP::noack)
        .onReceived([correlation, &connection](const AMQP::Message& m, uint64_t, bool) {
            if (m.correlationID() != correlation)
                return; // just skip it
            std::cout << "Got " << std::string(m.body(), m.bodySize()) << std::endl;
            connection.close();
        }
    );

    ev_run(loop, 0);

    return 0;
}