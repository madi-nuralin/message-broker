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
	MessageBroker broker;

	broker.subscribe({
 		.queue = {.name = "hello", .declare = true}
   		.on_receive = [](const auto& message){
			g_message("[x] Received b'%s'", message.getBody().c_str());
		});

	broker.subscribe({
		.queue = {.name = "rpc_queue", .declare = true},
		.message = "10",
		.on_receive = [](const auto& request, auto& response){
			auto n = std::stoi(request.getBody());
			g_message("[.] fib('%d')", n);
			response.setBody(std::to_string(fib(n)));
			return true;
		});

	while(1);**/

	return 0;
}
