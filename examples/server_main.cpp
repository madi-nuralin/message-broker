#include <iostream>
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
	MessageBroker broker("localhost", 5672, "guest", "guest", "/");

	broker.subscribe({
			.queue = {
				.name = "hello",
				.declare = true
			},
			.on_error = [](const auto& e) {
				std::cerr << e << std::endl;
			}
		}, [](const auto& message) {
			std::cout << "[x] Received b'" << message.getBody() << "'" << std::endl;
		}
	);

	broker.subscribe({
			.queue = {
				.name = "rpc_queue",
				.declare = true
			},
			.on_error = [](const auto& e) {
				std::cerr << e << std::endl;
			}
		}, [](const auto& request, auto& response){
			auto number = std::stoi(request.getBody());
			std::cout << "[.] fib(" <<  number << ")" << std::endl;
			response.setBody(std::to_string(fib(number)));
			return true;
		}
	);

	while(1);/**/

	return 0;
}