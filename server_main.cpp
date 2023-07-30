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

#if 0
	broker.subscribe("amq.direct", "test", [](const auto &statement) {
		std::cout << statement.serializeBody() << std::endl;
	});

	broker.subscribe("amq.direct", "test", [](const auto &statement) {
		std::cout << statement.serializeBody() << std::endl;
	});
#else
	broker.subscribe("amq.direct", "test",[](const auto &request, auto &response) {
		printf("1 request: %s\n", request.serialize().c_str());
		response.setReason("user not found");
		printf("1 response: %s\n", response.serialize().c_str());
	});

	broker.subscribe("amq.direct", "test2", [](const auto &request, auto &response) {
		printf("2 request: %s\n", request.serialize().c_str());
		response.setReason("user not found");
		printf("2 response: %s\n", response.serialize().c_str());
	});
#endif
	while(1){}

	return 0;
}