#include <iostream>
#include <thread>
#include <glib.h>
#include "../message_broker.hpp"

using namespace gammasoft;

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

int main(int argc, char const *argv[])
{
	/**/MessageBroker broker;

	broker.publish({
		.queue = {.name = "hello", .declare = true},
		.routing_key = "hello"
	}, "hello");

	broker.publish({
		.queue = {.exclusive = true, .declare = true},
		.routing_key = "rpc_queue"
	}, "30", [](const auto& response) {
		g_message("[.] Got  fib(%d) = %s", 30, response.getBody().c_str());
	});

	broker.subscribe({
		.queue = {.name = "rpc_queue", .declare = true}
	}, [](const auto& request, auto& response){
		auto n = std::stoi(request.getBody());
		g_message("[.] fib('%d')", n);
		response.setBody(std::to_string(fib(n)));
		return true;
	});

	while(1){}/**/

	/*Connection connection("localhost", 5672);

	std::thread t1([&](){
		Channel channel(&connection);
		channel.setup("rpc_queue");
		channel.consume("rpc_queue", [](auto& channel, const auto& envelope) {
			std::cout << (int)envelope.channel << " ";
			std::cout << std::string((char*)envelope.message.body.bytes, envelope.message.body.len) << std::endl;
			
			std::string reply_to((char*)envelope.message.properties.reply_to.bytes, envelope.message.properties.reply_to.len);
			std::string correlation_id((char*)envelope.message.properties.correlation_id.bytes, envelope.message.properties.correlation_id.len);
			
			Message message("330");
			message.setProperty("Content-Type", "text/plain");
			message.setProperty("Correlation-Id", correlation_id.c_str());

			channel.publish("", reply_to, message);
		});
	});

	std::thread t2([&](){
		Channel channel(&connection);
		channel.setup_queue("cats");
		channel.consume("cats", [](auto& channel, const auto& envelope) {
			std::cout << (int)envelope.channel << " ";
			std::cout << std::string((char*)envelope.message.body.bytes, envelope.message.body.len) << std::endl;
		});
	});

	std::thread t3([&](){
		Channel channel(&connection);
		channel.setup_queue("lions");
		channel.consume("lions", [](auto& channel, const auto& envelope) {
			std::cout << (int)envelope.channel << " ";
			std::cout << std::string((char*)envelope.message.body.bytes, envelope.message.body.len) << std::endl;
		});
	});

	t1.join();
	t2.join();
	t3.join();**/
}
