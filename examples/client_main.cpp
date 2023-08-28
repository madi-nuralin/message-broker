#include <iostream>
#include <thread>
#include <iostream>
#include "../message_broker.hpp"

using namespace gammasoft;

int main(int argc, char const *argv[])
{
	MessageBroker broker("localhost", 5672, "guest", "guest", "/");

	broker.publish({
			.queue = {.name = "hello",.declare = true},
			.routing_key = "hello",
			.on_error = [](const auto& e) {
				std::cerr << e << std::endl;
			}
		}, "hello"
	);

	broker.publish({
			.queue = {.exclusive = true,.declare = true},
			.routing_key = "rpc_queue",
			.on_error = [](const auto& e) {
				std::cerr << e << std::endl;
			}
		}, "30", [](const auto& response) {
			std::cout << "[.] Got  fib(" << 30 << ") = " <<  response.getBody() << std::endl;
		}
	);
while(1){}
//	broker.wait();

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
