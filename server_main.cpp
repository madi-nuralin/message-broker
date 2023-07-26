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

int main(int argc, char const *argv[])
{
	MessageBroker broker;

	broker.subscribe("amq.direct", "test", [](const auto &statement) {
		fprintf(stdout, "%s\n", statement.serializeBody().c_str());
	});

	broker.subscribe("amq.direct", "test", [](const auto &statement) {
		fprintf(stdout, "%s\n", statement.serializeBody().c_str());
	});

	/*broker.subscribe("amq.direct", "test", [](const auto &request, auto &response) {
		printf("request: %s\n", request.serialize().c_str());
		response.setReason("user not found");
		printf("response: %s\n", response.serialize().c_str());
	});

	broker.subscribe("amq.direct", "test2", [](const auto &request, auto &response) {
		printf("request: %s\n", request.serialize().c_str());
		response.setReason("user not found");
		printf("response: %s\n", response.serialize().c_str());
	});*/

	while(1){}

	return 0;
}