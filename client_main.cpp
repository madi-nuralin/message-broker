#include <iostream>
#include <thread>

#include "common.h"

int main(int argc, char const *argv[])
{
	Connection connection("localhost", 5672);

	std::thread t1([&](){
		Channel channel(&connection);
		channel.setup_queue("lions");
		channel.consume("lions", [](const auto& envelope) {
			std::cout << (int)envelope.channel << " ";
			std::cout << std::string((char*)envelope.message.body.bytes, envelope.message.body.len) << std::endl;
		});
	});

	std::thread t2([&](){
		Channel channel(&connection);
		channel.setup_queue("cats");
		channel.consume("cats", [](const auto& envelope) {
			std::cout << (int)envelope.channel << " ";
			std::cout << std::string((char*)envelope.message.body.bytes, envelope.message.body.len) << std::endl;
		});
	});

	t1.join();
	t2.join();
}
