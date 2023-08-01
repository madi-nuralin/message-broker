#include <vector>
#include <iostream>

#include <glib-object.h>
#include <json-glib/json-glib.h>
#include <json-glib/json-gobject.h>

#include "message_broker.hpp"
#include "user.hpp"
#include <chrono>
#include <thread>

std::vector<User> users({
	{1, "Alan Turing"},
	{2, "Keith Williams"}
});

using namespace gammasoft;

int main(int argc, char const *argv[])
{
	VistaMessageBroker broker;

	broker.subscribe({
		.queue = {.name = "cats"}
	}, [](const auto& message){
		std::cout << message.serialize() << std::endl;
	});

	broker.subscribe({
		.queue = {.name = "rpc_queue"}
	}, [](const auto& request, auto& response){
		response.setReason("perror");
		std::cout << request.serialize() << std::endl;
	});

	while(1);

	return 0;
}