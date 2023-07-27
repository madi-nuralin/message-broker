#include <stdio.h>
#include <assert.h>
#include <thread>
#include <stdexcept>
#include <iostream>
#include <sstream>

#include <glib-object.h>
#include <json-glib/json-glib.h>
#include <json-glib/json-gobject.h>

#include "message_broker.hpp"
#include "utils.h"

namespace gammasoft {

const char ALPHABET[] = {
    "0123456789"
    "abcdefgjhiklmnopqrstvwxyz"
    "ABCDEFGJHIKLMNOPQRSTVWXYZ"
};

static std::string generateReqId()
{
	std::string result;
	for(std::size_t i = 0; i < 16; i++) {
		result += ALPHABET[rand()%(sizeof(ALPHABET)-1)];
	}
	return result;
}

static void build_json_from_json_reader(JsonReader *reader, JsonBuilder *builder)
{
	for (int i = 0; i < json_reader_count_members(reader); ++i) {
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

VistaMessageBroker::Connection::Connection(
	const std::string &host, int port,
	const std::string &username,
	const std::string &password,
	const std::string &vhost, int frame_max)
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
			username.c_str(), password.c_str()),
		"Logging in");
	std::stringstream ss;
	ss << std::this_thread::get_id();
	uint64_t id = std::stoull(ss.str());

	amqp_channel_open(conn, channel=id);
	die_on_amqp_error(amqp_get_rpc_reply(conn), "Opening channel");
}

VistaMessageBroker::Connection::~Connection()
{
	die_on_amqp_error(amqp_channel_close(conn, channel, AMQP_REPLY_SUCCESS),
			"Closing channel");
	die_on_amqp_error(amqp_connection_close(conn, AMQP_REPLY_SUCCESS),
			"Closing connection");
	die_on_error(amqp_destroy_connection(conn), "Ending connection");
}

void VistaMessageBroker::Connection::declareExchange(VistaMessageBroker::Exchange &exchange)
{
	amqp_exchange_declare(conn, channel,
		exchange.name.empty()
			? amqp_empty_bytes
			: amqp_cstring_bytes(exchange.name.c_str()),
		exchange.type.empty()
			? amqp_empty_bytes
			: amqp_cstring_bytes(exchange.type.c_str()),
		exchange.passive, exchange.durable, exchange.auto_delete,
		exchange.internal, amqp_empty_table);
	die_on_amqp_error(amqp_get_rpc_reply(conn), "Declaring exchange");
}

void VistaMessageBroker::Connection::declareQueue(VistaMessageBroker::Queue &queue)
{
	amqp_queue_declare_ok_t *r = amqp_queue_declare(conn, channel,
		queue.name.empty()
			? amqp_empty_bytes
			: amqp_cstring_bytes(queue.name.c_str()),
		queue.passive, queue.durable, queue.exclusive, queue.auto_delete, amqp_empty_table);
	die_on_amqp_error(amqp_get_rpc_reply(conn), "Declaring queue");
	amqp_bytes_t queuename = amqp_bytes_malloc_dup(r->queue);
	if (queuename.bytes == NULL) {
		throw std::runtime_error("Out of memory while copying queue name");
	}
	queue.name = std::string((char *)queuename.bytes, (int)queuename.len);
}

void VistaMessageBroker::Connection::bindQueue(const std::string &queuename,
	const std::string &exchange, const std::string &routingkey)
{
	amqp_queue_bind(conn, channel, 
		queuename.empty()
			? amqp_empty_bytes
			: amqp_cstring_bytes(queuename.c_str()),
		exchange.empty()
			? amqp_empty_bytes
			: amqp_cstring_bytes(exchange.c_str()),
		routingkey.empty()
			? amqp_empty_bytes
			: amqp_cstring_bytes(routingkey.c_str()),
		amqp_empty_table);
	die_on_amqp_error(amqp_get_rpc_reply(conn), "Binding queue");
}

void VistaMessageBroker::Connection::basicPublish(const std::string &exchange,
	const std::string &routingkey, const Message &message, bool mandatory, bool immediate)
{
	die_on_error(amqp_basic_publish(conn, channel,
		exchange.empty()
			? amqp_empty_bytes
			: amqp_cstring_bytes(exchange.c_str()),
		routingkey.empty()
			? amqp_empty_bytes
			: amqp_cstring_bytes(routingkey.c_str()),
		mandatory, immediate, &message.properties, message.body),
		"Publishing");
}

void VistaMessageBroker::Connection::basicConsume(const std::string &queuename, const std::string &consumer_tag,
	bool no_local, bool no_ack, bool exclusive, uint16_t message_prefetch_count)
{
	amqp_basic_consume(conn, channel,
		queuename.empty()
			? amqp_empty_bytes
			: amqp_cstring_bytes(queuename.c_str()),
		consumer_tag.empty()
			? amqp_empty_bytes
			: amqp_cstring_bytes(consumer_tag.c_str()),
		no_local, no_ack, exclusive, amqp_empty_table);
	die_on_amqp_error(amqp_get_rpc_reply(conn), "Consuming");
}

VistaMessageBroker::Envelope
	VistaMessageBroker::Connection::consumeMessage(int timeout, int flags)
{
	amqp_rpc_reply_t res;
	amqp_envelope_t envelope;

	amqp_maybe_release_buffers(conn);

	res = amqp_consume_message(conn, &envelope, NULL, 0);

	if (AMQP_RESPONSE_NORMAL != res.reply_type) {
		die_on_amqp_error(res, "error reply from a RPC method");
	}

	return Envelope(envelope);
}

VistaMessageBroker::VistaMessageBroker(const std::string &host, int port,
	const std::string &username, const std::string &password, const std::string &vhost, int frame_max)
{
	std::srand(std::time(NULL));

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
}

VistaMessageBroker::~VistaMessageBroker()
{
	//
}

/*void VistaMessageBroker::publish(Configuration configuration,
	const std::string &routingkey, const std::string &messagebody)
{
	Connection connection(m_host, m_port, m_username, m_password, m_vhost, m_frame_max);

	Statement statement
	statement.setBody(messagebody);

	Message message(statement.serialize());
	message.setProperty("Content-Type", "application/json");
	message.setProperty("Delivery-Mode", (uint8_t)2);	

	auto exchange = configuration.exchange;
	auto queue = configuration.queue;
	
	connection->declareExchange(exchange);
	connection->declareQueue(queue);
	connection->bindQueue(queue.name, exchange.name, routingkey);
	connection->basicPublish(exchange.name, routingkey, message);
}*/

void VistaMessageBroker::publish(const std::string &exchange, const std::string &routingkey, const std::string &messagebody)
{
	Connection connection(m_host, m_port, m_username, m_password, m_vhost, m_frame_max);

	Statement statement;
	statement.setBody(messagebody);

	Message message(statement.serialize());
	message.setProperty("Content-Type", "application/json");
	message.setProperty("Delivery-Mode", (uint8_t)2);

	connection.basicPublish(exchange, routingkey, message);
}

void VistaMessageBroker::publish(const std::string &exchange, const std::string &routingkey,
	const std::string &messagebody, void (*callback)(const Response& response))
{
	std::thread worker([&]()
	{
		Connection connection(m_host, m_port, m_username, m_password, m_vhost, m_frame_max);
		Queue queue;
		connection.declareQueue(queue);

		Request request;
		request.setBody(messagebody);

		Message message(request.serialize());
		message.setProperty("Content-Type", "application/json");
		message.setProperty("Delivery-Mode", (uint8_t)2);
		message.setProperty("Correlation-Id", request.reqid().c_str());
		message.setProperty("Reply-To", queue.name.c_str());

		connection.basicPublish(exchange, routingkey, message);
		connection.basicConsume(queue.name, "", false, true, false);

		{
			amqp_frame_t frame;
			int result;

			amqp_basic_deliver_t *d;
			amqp_basic_properties_t *p;
			size_t body_target;
			size_t body_received;
			amqp_connection_state_t conn = connection.conn;

			for(;;) {
				amqp_maybe_release_buffers(conn);
				result = amqp_simple_wait_frame(conn, &frame);
				printf("Result: %d\n", result);
				if (result < 0) {
					break;
				}

				printf("Frame type: %u channel: %u\n", frame.frame_type, frame.channel);
				if (frame.frame_type != AMQP_FRAME_METHOD) {
					continue;
				}

				printf("Method: %s\n", amqp_method_name(frame.payload.method.id));
				if (frame.payload.method.id != AMQP_BASIC_DELIVER_METHOD) {
					continue;
				}

				d =(amqp_basic_deliver_t *)frame.payload.method.decoded;
				printf("Delivery: %u exchange: %.*s routingkey: %.*s\n",
					(unsigned)d->delivery_tag,(int)d->exchange.len,
					(char *)d->exchange.bytes,(int)d->routing_key.len,
					(char *)d->routing_key.bytes);

				result = amqp_simple_wait_frame(conn, &frame);
				if (result < 0) {
					break;
				}

				if (frame.frame_type != AMQP_FRAME_HEADER) {
					fprintf(stderr, "Expected header!");
					abort();
				}
				p = (amqp_basic_properties_t *)frame.payload.properties.decoded;
				if (p->_flags & AMQP_BASIC_CONTENT_TYPE_FLAG) {
					printf("Content-type: %.*s\n",(int)p->content_type.len,
						(char *)p->content_type.bytes);
				}
				printf("----\n");

				body_target =(size_t)frame.payload.properties.body_size;
				body_received = 0;

				while(body_received < body_target) {
					result = amqp_simple_wait_frame(conn, &frame);
					if (result < 0) {
						break;
					}

					if (frame.frame_type != AMQP_FRAME_BODY) {
						fprintf(stderr, "Expected body!");
						abort();
					}

					body_received += frame.payload.body_fragment.len;
					assert(body_received <= body_target);

					amqp_dump(frame.payload.body_fragment.bytes,
					    frame.payload.body_fragment.len);
			    }

				if (body_received != body_target) {
					/* Can only happen when amqp_simple_wait_frame returns <= 0 */
					/* We break here to close the connection */
					break;
				}

				callback(Response(std::string((char *)frame.payload.body_fragment.bytes, frame.payload.body_fragment.len)));

				/* everything was fine, we can quit now because we received the reply */
				break;
			}
		}
	});

	worker.detach();
}

void VistaMessageBroker::subscribe(const std::string &exchange,
	const std::string &bindingkey, void (*callback)(const Statement& statement))
{
	std::thread worker([&]()
	{
		Connection connection(m_host, m_port, m_username, m_password, m_vhost, m_frame_max);
		Queue queue;

		connection.declareQueue(queue);
		connection.bindQueue(queue.name, exchange, bindingkey);
		connection.basicConsume(queue.name, "", false, true, false);

		for(;;) {
			auto envelope = connection.consumeMessage();

			callback(Statement(std::string((char *)envelope.message.body.bytes, envelope.message.body.len)));
		}
	});

	worker.detach();
}

void VistaMessageBroker::subscribe(const std::string &exchange,
	const std::string &bindingkey, void (*callback)(const Request &request, Response &response))
{
	std::thread worker([&]()
	{
		Connection connection(m_host, m_port, m_username, m_password, m_vhost, m_frame_max);
		Queue queue;

		connection.declareQueue(queue);
		connection.bindQueue(queue.name, exchange, bindingkey);
		connection.basicConsume(queue.name, "", false, true, false);

		for(;;) {
			auto envelope = connection.consumeMessage();

			/*
				send reply
			*/

			{
				Request request(std::string((char *)envelope.message.body.bytes, envelope.message.body.len));
				Response response(request);

				callback(request, response);

				std::string reply_to((char *)envelope.message.properties.reply_to.bytes, (int)envelope.message.properties.reply_to.len);
				std::string correlation_id((char *)envelope.message.properties.correlation_id.bytes, (int)envelope.message.properties.correlation_id.len);

				Message message(response.serialize());
				message.setProperty("Content-Type", "application/json");
				message.setProperty("Delivery-Mode", (uint8_t)2);
				message.setProperty("Reply-To", reply_to.c_str());
				message.setProperty("Correlation-Id", correlation_id.c_str());

				connection.basicPublish("", reply_to, message);
			}
		}
	});

	worker.detach();
}

VistaMessageBroker::Statement::Statement()
	: m_reqid(generateReqId()), m_type("message")
{
	m_body = json_node_new(JSON_NODE_NULL);
}

VistaMessageBroker::Statement::~Statement()
{
	if (m_body) {
		json_node_free(m_body);
	}
}


VistaMessageBroker::Statement::Statement(const std::string &str)
	: VistaMessageBroker::Statement::Statement()
{
	g_autoptr(JsonParser) parser = json_parser_new();
	json_parser_load_from_data(parser, (gchar*)str.c_str(), -1, NULL);

	g_autoptr(JsonReader) reader = json_reader_new(json_parser_get_root(parser));

	json_reader_read_member(reader, "reqid");
	m_reqid = json_reader_get_string_value(reader);
	json_reader_end_member(reader);

	json_reader_read_member(reader, "type");
	m_type = json_reader_get_string_value(reader);
	json_reader_end_member(reader);

	g_autoptr(JsonBuilder) builder = json_builder_new();

	json_reader_read_member(reader, "body");
	json_builder_begin_object(builder);

	build_json_from_json_reader(reader, builder);
	
	json_builder_end_object(builder);
	json_reader_end_member(reader);

	std::string error;
	if (!this->setBody(json_builder_get_root(builder), &error)) {
		throw std::runtime_error(error);
	}
}

VistaMessageBroker::Response::Response(const std::string &str)
	: VistaMessageBroker::Statement::Statement(str)
{
	g_autoptr(JsonParser) parser = json_parser_new();
	json_parser_load_from_data(parser, (gchar*)str.c_str(), -1, NULL);

	g_autoptr(JsonReader) reader = json_reader_new(json_parser_get_root(parser));

	json_reader_read_member(reader, "reason");
	m_reason = json_reader_get_string_value(reader);
	json_reader_end_member(reader);
}

bool VistaMessageBroker::Statement::setBody(const std::string &body, std::string *error)
{
	g_autoptr(JsonParser) parser = json_parser_new();
	g_autoptr(GError) gerror = nullptr;

	if (!json_parser_load_from_data(parser, (gchar*)body.c_str(), -1, &gerror)) {
		if (error) {
			*error = std::string("Cannot parse body: ") + (char *)gerror->message;
		}
		return false;
	}

	return this->setBody(json_parser_get_root(parser), error);
}

bool VistaMessageBroker::Statement::setBody(const JsonNode* json_node, std::string *error)
{
	if (m_body) {
		json_node_free(m_body);
	}

	m_body = json_node_copy((JsonNode*)json_node);

	return true;
}

static std::string _serialize(const std::string &reqid, const std::string &type, const JsonNode *body, const std::string &reason = "")
{
	g_autoptr(JsonBuilder) builder = json_builder_new();
	
	json_builder_begin_object(builder);

	json_builder_set_member_name(builder, "reqid");
	json_builder_add_string_value(builder, reqid.c_str());

	json_builder_set_member_name(builder, "type");
	json_builder_add_string_value(builder, type.c_str());

	json_builder_set_member_name(builder, "body");
	json_builder_add_value(builder, json_node_copy((JsonNode*)body));

	if (!reason.empty()) {
		json_builder_set_member_name(builder, "reason");
		json_builder_add_string_value(builder, reason.c_str());
	}

	json_builder_end_object(builder);

	g_autoptr(JsonNode) root = json_builder_get_root(builder);

	g_autoptr(JsonGenerator) gen = json_generator_new();

	json_generator_set_root(gen, root);

	return std::string(json_generator_to_data(gen, NULL));
}

std::string VistaMessageBroker::Statement::serialize() const
{
	return _serialize(m_reqid, m_type, m_body);
}

std::string VistaMessageBroker::Response::serialize() const
{
	return _serialize(m_reqid, m_type, m_body, m_reason);
}

std::string VistaMessageBroker::Statement::serializeBody() const
{
	g_autoptr(JsonGenerator) gen = json_generator_new();
	json_generator_set_root(gen, m_body);

	return std::string(json_generator_to_data(gen, NULL));
}

void VistaMessageBroker::Response::setReason(const std::string &reason)
{
	m_type = reason.empty() ? "response" : "error";
	m_reason = reason;
}

} // end namespace gammasoft