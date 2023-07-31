#include <iostream>
#include <thread>

#include "common.h"

int main(int argc, char const *argv[])
{
	Connection connection("localhost", 5672);

	std::thread t1([&](){
		Channel channel(&connection);
		auto queue = channel.setup_queue("lions");
		channel.consume("lions");
	});

	std::thread t2([&](){
		Channel channel(&connection);
		channel.setup_queue("cats");
		channel.consume("cats");
	});

	t1.join();
	t2.join();
}
