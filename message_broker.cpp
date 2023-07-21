#include <stdio.h>
#include <assert.h>
#include <thread>

#include <glib-object.h>
#include <json-glib/json-glib.h>
#include <json-glib/json-gobject.h>

#include "message_broker.hpp"
#include "utils.h"


const char ALPHABET[] = {
    "0123456789"
    "abcdefgjhiklmnopqrstvwxyz"
    "ABCDEFGJHIKLMNOPQRSTVWXYZ"
};

std::string generateReqId()
{
    std::string result;
    for(std::size_t i = 0; i < 16; i++) {
        result += ALPHABET[rand()%(sizeof(ALPHABET)-1)];
    }
    return result;
}

MessageBroker::Connection::Connection(const std::string &host = "localhost", int port = 5672,
                                      const std::string &username = "guest",
                                      const std::string &password = "guest",
                                      const std::string &vhost = "/", int frame_max = 131072)
{
	conn = amqp_new_connection();

	socket = amqp_tcp_socket_new(conn);
	if (!socket) {
		die("creating TCP socket");
	}

	int status = amqp_socket_open(socket, host.c_str(), port);
	if (status) {
		die("opening TCP socket");
	}

	die_on_amqp_error(amqp_login(conn, vhost.c_str(), 0, frame_max, 0, AMQP_SASL_METHOD_PLAIN,
			user.c_str(), password.c_str()),
		"Logging in");
	amqp_channel_open(conn, 1);
	die_on_amqp_error(amqp_get_rpc_reply(conn), "Opening channel");
}

MessageBroker::Connection::~Connection()
{
	die_on_amqp_error(amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS),
			"Closing channel");
	die_on_amqp_error(amqp_connection_close(conn, AMQP_REPLY_SUCCESS),
			"Closing connection");
	die_on_error(amqp_destroy_connection(conn), "Ending connection");
}

MessageBroker::MessageBroker(const std::string &host = "localhost", int port = 5672,
                             const std::string &username = "guest",
                             const std::string &password = "guest",
                             const std::string &vhost = "/", int frame_max = 131072)
{
	if (host.empty()) {
		throw std::runtime_error("host is not specified, it is required");
	}
	if (vhost.empty()) {
		throw std::runtime_error("vhost is not specified, it is required");
	}
	if (port <= 0) {
		throw std::runtime_error("port is not valid, it must be a positive number");
	}

	m_host = host;
	m_port = port;
	m_username = username;
	m_password = password;
	m_vhost = vhost;
	m_frame_max = frame_max;

	m_connection = std::make_shared<Connection>(host, port, username, password, vhost, frame_max);
}

MessageBroker::~MessageBroker()
{

}

void basicPublish(const std::string &exchange,
                  const std::string &routingkey,
                  const std::string &messagebody,
                  amqp_connection_state_t &conn)
{
	amqp_basic_properties_t props;
    props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
    props.content_type = amqp_cstring_bytes("text/plain");
    props.delivery_mode = 2; /* persistent delivery mode */
    die_on_error(amqp_basic_publish(conn, 1, amqp_cstring_bytes(exchange.c_str()),
                                    amqp_cstring_bytes(routingkey.c_str()), 0, 0,
                                    &props, amqp_cstring_bytes(messagebody.c_str())),
                 "Publishing");
}

void MessageBroker::publish(const std::string &exchange,
                            const std::string &routingkey,
                            const std::string &messagebody)
{
	basicPublish(m_connection->conn, exchange, routingkey, Envelope(messagebody).serialize());
}

void MessageBroker::subscribe(const std::string& bindingkey,
                              void (*callback)(const Envelope& envelope))
{
	{
		amqp_queue_declare_ok_t *r = amqp_queue_declare(
			conn, 1, amqp_empty_bytes, 0, 0, 0, 1, amqp_empty_table);
		die_on_amqp_error(amqp_get_rpc_reply(conn), "Declaring queue");
		queuename = amqp_bytes_malloc_dup(r->queue);
		if (queuename.bytes == NULL) {
			fprintf(stderr, "Out of memory while copying queue name");
			return 1;
		}
	}

	amqp_queue_bind(conn, 1, queuename, amqp_cstring_bytes(exchange),
	amqp_cstring_bytes(bindingkey), amqp_empty_table);
	die_on_amqp_error(amqp_get_rpc_reply(conn), "Binding queue");

	amqp_basic_consume(conn, 1, queuename, amqp_empty_bytes, 0, 1, 0,
		amqp_empty_table);
	die_on_amqp_error(amqp_get_rpc_reply(conn), "Consuming");

	{
	for (;;) {
	amqp_rpc_reply_t res;
	amqp_envelope_t envelope;

	amqp_maybe_release_buffers(conn);

	res = amqp_consume_message(conn, &envelope, NULL, 0);

	if (AMQP_RESPONSE_NORMAL != res.reply_type) {
	break;
	}

	printf("Delivery %u, exchange %.*s routingkey %.*s\n",
	(unsigned)envelope.delivery_tag, (int)envelope.exchange.len,
	(char *)envelope.exchange.bytes, (int)envelope.routing_key.len,
	(char *)envelope.routing_key.bytes);

	if (envelope.message.properties._flags & AMQP_BASIC_CONTENT_TYPE_FLAG) {
	printf("Content-type: %.*s\n",
	(int)envelope.message.properties.content_type.len,
	(char *)envelope.message.properties.content_type.bytes);
	}
	printf("----\n");

	amqp_dump(envelope.message.body.bytes, envelope.message.body.len);

	amqp_destroy_envelope(&envelope);
	}
	}

	amqp_bytes_free(queuename);
}

void build_json_from_json_reader(JsonReader *reader, JsonBuilder *builder)
{
	for (int i = 0; i < json_reader_count_members(reader); ++i)
	{
		json_reader_read_element(reader, i);
		json_builder_set_member_name(builder, json_reader_get_member_name(reader));
		if (json_reader_is_object(reader)) {
			json_builder_begin_object(builder);
			build_json_from_json_reader(reader, builder);
			json_builder_end_object(builder);
		} else {
			json_builder_add_value(builder, json_node_copy(json_reader_get_value(reader)));
		}
		json_reader_end_element(reader);
	}
}

MessageBroker::QueryInterface::QueryInterface()
{
	//
}

MessageBroker::QueryInterface::QueryInterface(const char* json_str) 
	: MessageBroker::QueryInterface()
{
	g_autoptr(JsonParser) parser = json_parser_new();
	json_parser_load_from_data(parser, json_str, -1, NULL);

	g_autoptr(JsonReader) reader = json_reader_new(json_parser_get_root(parser));

	json_reader_read_member(reader, "type");
	setType(json_reader_get_string_value(reader));
	json_reader_end_member(reader);

	g_autoptr(JsonBuilder) builder = json_builder_new();

	json_reader_read_member(reader, "body");
	{
		json_builder_begin_object(builder);
		{
			build_json_from_json_reader(reader, builder);
		}
		json_builder_end_object(builder);
	}
	json_reader_end_member(reader);

	setBody(json_builder_get_root(builder));
}

bool MessageBroker::QueryInterface::setType(const char* type)
{
	const char *arr[] = {
		MessageBroker::QueryInterface::QUERY_ERROR,
		MessageBroker::QueryInterface::QUERY_RESPONSE,
		MessageBroker::QueryInterface::QUERY_REQUEST
	};

	for (int i = 0; i < 3; ++i) {
		if (strcmp(type, arr[i]) == 0) {
			m_type = (char *)arr[i];
			return true;
		}
	}

	return false;
}

bool MessageBroker::QueryInterface::setBody(const char* json_str)
{
	g_autoptr(JsonParser) parser = json_parser_new();
	g_autoptr(GError) error = nullptr;

	if (!json_parser_load_from_data(parser, (gchar*)json_str, -1, &error)) {
		g_warning("Cannot parse json_str: %s", error->message);
		return false;
	}

	setBody(json_parser_get_root(parser));

	return true;
}

bool MessageBroker::QueryInterface::setBody(const JsonNode* json_node)
{
	m_body = json_node_copy((JsonNode*)json_node);

	return true;
}

char* MessageBroker::QueryInterface::serialize() const
{
	g_autoptr(JsonBuilder) builder = json_builder_new();
	
	json_builder_begin_object(builder);

	json_builder_set_member_name(builder, "type");
	json_builder_add_string_value(builder, getType());

	json_builder_set_member_name(builder, "body");
	json_builder_add_value(builder, json_node_copy(getBody()));

	json_builder_end_object(builder);

	g_autoptr(JsonNode) root = json_builder_get_root(builder);
	g_autoptr(JsonGenerator) gen = json_generator_new();
	json_generator_set_root(gen, root);

	return json_generator_to_data(gen, NULL);
}

char* MessageBroker::QueryInterface::serializeBody() const
{
	g_autoptr(JsonGenerator) gen = json_generator_new();
	json_generator_set_root(gen, getBody());

	return json_generator_to_data(gen, NULL);
}
