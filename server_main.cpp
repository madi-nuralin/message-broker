#include <vector>
#include <iostream>

#include <glib-object.h>
#include <json-glib/json-glib.h>
#include <json-glib/json-gobject.h>

#include "message_broker.hpp"
#include "user.hpp"

std::vector<User> users({
	{1, "Alan Turing"},
	{2, "Keith Williams"}
});

int main(int argc, char const *argv[])
{
	MessageBroker broker;

	/*broker.subscribe("amq.direct", "test", [](const MessageBroker::Message &message) {
		fprintf(stdout, "%s\n", message.serializeBody().c_str());
	});*/

	broker.subscribe("amq.direct", "test", [](const MessageBroker::Request &request, MessageBroker::Response &response) {
		printf("request: %s\n", request.serialize().c_str());
		response.setReason("user not found");
		printf("response: %s\n", response.serialize().c_str());
	});

	return 0;
}