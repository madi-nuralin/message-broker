#include <iostream>
#include <thread>

#include "common.h"

int main(int argc, char const *argv[])
{
	Connection connection("localhost", 5672);
	std::thread t1([&](){
		Channel channel(&connection);

		channel.consume();
	});

	std::thread t2([&](){
		Channel channel(&connection);
		
		channel.consume();
	});

	t1.join();
	t2.join();
}
