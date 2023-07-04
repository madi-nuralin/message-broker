#include <vector>
#include <iostream>

#include <glib-object.h>
#include <json-glib/json-glib.h>
#include <json-glib/json-gobject.h>

#include "message-broker.hpp"
#include "user.hpp"

std::vector<User> users({
	{1, "Alan Turing"},
	{2, "Keith Williams"}
});

int main(int argc, char const *argv[])
{
	MessageBroker msgBroker("localhost", 5672);

	msgBroker.listen("amq.direct", "logs", 
		[users](const MessageBroker::Request &request, MessageBroker::Response &response)
	{
		g_autoptr(JsonParser) parser = json_parser_new();
		json_parser_load_from_data(parser, (gchar*)(request.serializeBody()), -1, NULL);
		g_autoptr(JsonReader) reader = json_reader_new(json_parser_get_root(parser));

		json_reader_read_member(reader, "query_name");
		auto queryName = json_reader_get_string_value(reader);
		json_reader_end_member(reader);

		json_reader_read_member(reader, "query_data");
		json_reader_read_element(reader, 0);
		auto userId = json_reader_get_int_value(reader);
		json_reader_end_element(reader);
		json_reader_end_member(reader);

		std::cout << queryName << std::endl;
		for (const auto &user : users) {
			if (user.id == userId) {
				response.setBody(users[0].serialize());
				return true;
			}
		}

		response.setBody("{\"reason\":\"could not find user with id\"}");		
		return false;
	});

	return 0;
}