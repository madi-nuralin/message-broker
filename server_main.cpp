#include <vector>
#include <iostream>

#include <glib-object.h>
#include <json-glib/json-glib.h>
#include <json-glib/json-gobject.h>

#include "message-broker.hpp"

struct User {
	int id;
	std::string name;

	const char* serialize() {
		JsonBuilder *builder = json_builder_new();
		json_builder_begin_object(builder);

		json_builder_set_member_name(builder, "id");
		json_builder_add_int_value(builder, id);

		json_builder_set_member_name(builder, "name");
		json_builder_add_string_value(builder, name.c_str());
		
		json_builder_end_object(builder);

		JsonGenerator *gen = json_generator_new();
		JsonNode *root = json_builder_get_root(builder);
		json_generator_set_root(gen, root);
		gchar *json_str = json_generator_to_data(gen, NULL);

		json_node_free(root);
		g_object_unref(gen);
		g_object_unref(builder);

		return (const char *)json_str;
	}
};

std::vector<User> users({
	{1, "Alan Turing"},
	{2, "Keith Williams"}
});

int main(int argc, char const *argv[])
{
	MessageBroker msgBroker("localhost", 5672);

	msgBroker.listen("amq.direct", "logs", [users](const MessageBroker::Request &request, MessageBroker::Response &response)
	{
		JsonParser *parser = json_parser_new();
		json_parser_load_from_data(parser, (gchar*)request.body.c_str(), -1, NULL);

		JsonReader *reader = json_reader_new(json_parser_get_root(parser));

		json_reader_read_member(reader, "query-name");
		auto queryName = json_reader_get_string_value(reader);
		json_reader_end_member(reader);

		json_reader_read_member(reader, "query-data");
		json_reader_read_member(reader, "iin");
		auto userId = json_reader_get_int_value(reader);
		json_reader_end_member(reader);
		json_reader_end_member(reader);

		g_object_unref(reader);
		g_object_unref(parser);

		std::cout << queryName << std::endl;
		for (const auto &user : users) {
			if (user.id == userId) {
				response.body = users[0].serialize();
				return true;
			}
		}

		response.type = MessageBroker::QueryInterface::QUERY_ERROR;
		response.body = "{\"reason\":\"could not find user with id\"}";
		
		return false;
	});

	while (1) {
		//msgBroker.wait()
	}

	return 0;
}