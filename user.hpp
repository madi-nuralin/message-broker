#pragma once

#include <string>

#include <glib-object.h>
#include <json-glib/json-glib.h>
#include <json-glib/json-gobject.h>

struct User {
	int id;
	std::string name;

	char* serialize() const
	{
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

		return (char *)json_str;
	}
};