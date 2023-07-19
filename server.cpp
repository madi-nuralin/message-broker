#include <iostream>
#include <algorithm>
#include <thread>
#include <chrono>
#include <amqpcpp.h>
#include <amqpcpp/libev.h>

int fib(int n)
{
    switch (n)
    {
    case 0:
        return 0;
    case 1:
        return 1;
    default:
        return fib(n - 1) + fib(n - 2);
    }
}

int main(int argc, char* argv[])
{
	auto *loop = EV_DEFAULT;

	AMQP::LibEvHandler handler(loop);

	AMQP::TcpConnection connection(&handler, AMQP::Address("localhost", 5672, AMQP::Login("guest", "guest"), "/"));

	AMQP::TcpChannel channel(&connection);

	channel.onError([](const char *message) {
		std::cerr << "channel error: " << message << std::endl;
	});

	channel.onReady([]() {
		//
	});

	channel.setQos(1);
	channel.declareQueue("rpc_queue");
	channel.consume("")
		.onReceived([&channel](const AMQP::Message& m, uint64_t tag, bool) {
			std::cout << "fib(" << std::string(m.body(), m.bodySize()) << ")" << std::endl;
			AMQP::Envelope env(std::to_string(fib(std::stoi(m.body()))));
			env.setCorrelationID(m.correlationID());

			channel.publish("", m.replyTo(), env);
			channel.ack(tag);
		}
	);

	ev_run(loop, 0);

	return 0;
}