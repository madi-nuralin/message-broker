#include <stdio.h>
#include <assert.h>
#include <thread>
#include <stdexcept>
#include <iostream>

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
	std::srand(std::time(nullptr));
    std::string result;
    for(std::size_t i = 0; i < 16; i++) {
        result += ALPHABET[rand()%(sizeof(ALPHABET)-1)];
    }
    return result;
}

MessageBroker::Connection::Connection(const std::string &host, int port,
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

void MessageBroker::Connection::declareExchange(MessageBroker::Exchange &exchange)
{
	amqp_exchange_declare(conn, 1,
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

void MessageBroker::Connection::declareQueue(MessageBroker::Queue &queue)
{
	amqp_queue_declare_ok_t *r = amqp_queue_declare(conn, 1, queue.name.empty()
			? amqp_empty_bytes : amqp_cstring_bytes(queue.name.c_str()),
		queue.passive, queue.durable, queue.exclusive, queue.auto_delete, amqp_empty_table);
	die_on_amqp_error(amqp_get_rpc_reply(conn), "Declaring queue");
	amqp_bytes_t queuename = amqp_bytes_malloc_dup(r->queue);
	if (queuename.bytes == NULL) {
		throw std::runtime_error("Out of memory while copying queue name");
	}
	queue.name = std::string((char *)queuename.bytes, (int)queuename.len);
}

void MessageBroker::Connection::bindQueue(const std::string &queuename,
                                          const std::string &exchange,
                                          const std::string &routingkey)
{
	amqp_queue_bind(conn, 1, 
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

void MessageBroker::Connection::basicPublish(const std::string &exchange,
                                             const std::string &routingkey,
                                             const BasicMessage &message,
                                             bool mandatory,
                                             bool immediate)
{
	const char* _message = "Hello";
	die_on_error(amqp_basic_publish(conn, 1,
		exchange.empty()
			? amqp_empty_bytes
			: amqp_cstring_bytes(exchange.c_str()),
		routingkey.empty()
			? amqp_empty_bytes
			: amqp_cstring_bytes(routingkey.c_str()),
		mandatory, immediate, &message.properties, message.body),
		"Publishing");
}

void MessageBroker::Connection::basicConsume(const std::string &queuename,
                                             const std::string &consumer_tag,
                                             bool no_local, bool no_ack,
                                             bool exclusive,
                                             uint16_t message_prefetch_count)
{
	amqp_basic_consume(conn, 1,
		queuename.empty()
			? amqp_empty_bytes
			: amqp_cstring_bytes(queuename.c_str()),
		consumer_tag.empty()
			? amqp_empty_bytes
			: amqp_cstring_bytes(consumer_tag.c_str()),
		no_local, no_ack, exclusive, amqp_empty_table);
	die_on_amqp_error(amqp_get_rpc_reply(conn), "Consuming");
}

MessageBroker::MessageBroker(const std::string &host, int port,
                             const std::string &username,
                             const std::string &password,
                             const std::string &vhost, int frame_max)
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

void MessageBroker::publish(MessageBroker::Exchange &exchange,
                            MessageBroker::Queue &queue,
                            const std::string &routingkey,
                            const std::string &messagebody)
{
	//MessageBroker::Message message(messagebody);

	//message.serialize();
	//message.serializeBody();
	
	m_connection->declareExchange(exchange);

	m_connection->declareQueue(queue);

	m_connection->bindQueue(queue.name, exchange.name, routingkey);
	
	m_connection->basicPublish(exchange.name, routingkey, BasicMessage(messagebody), false, false);

}

void MessageBroker::publish(const std::string &exchange,
                            const std::string &routingkey,
                            const std::string &messagebody)
{
	/*Properties props;
	props.setContentType("application/json");
	props.setDeliveryMode(2);/**/
	Message message(messagebody);
        //message.setProperty("Content-Type", "application/json);
	//message.setProperty("Delivery-Mode", 2);
	
	amqp_basic_properties_t props;
    props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
    props.content_type = amqp_cstring_bytes("text/plain");
    props.delivery_mode = 2; /* persistent delivery mode */

	m_connection->basicPublish(exchange, routingkey, BasicMessage(message.serialize(), props));
}

void MessageBroker::subscribe(const std::string &exchange,
	                          const std::string &bindingkey,
                              void (*callback)(const Message& message))
{
	Queue queue("");

	m_connection->declareQueue(queue);

	m_connection->bindQueue(queue.name, exchange, bindingkey);

	m_connection->basicConsume(queue.name, "", false, true, false);

	for(;;) {
		amqp_rpc_reply_t res;
		amqp_envelope_t envelope;

		amqp_maybe_release_buffers(m_connection->conn);

		res = amqp_consume_message(m_connection->conn, &envelope, NULL, 0);

		if (AMQP_RESPONSE_NORMAL != res.reply_type) {
			break;
		}

		callback(Message(std::string((char *)envelope.message.body.bytes, envelope.message.body.len)));

		amqp_destroy_envelope(&envelope);
	}
}

MessageBroker::Message::Message()
	: m_reqid(generateReqId()), m_type("message")
{
	m_body = json_node_new(JSON_NODE_NULL);
}

MessageBroker::Message::~Message()
{
	if (m_body) {
		json_node_free(m_body);
	}
}


MessageBroker::Message::Message(const std::string &body)
	: MessageBroker::Message::Message()
{
	std::string error;
	if (!this->setBody(body, &error)) {
		throw std::runtime_error(error);
	}
}

bool MessageBroker::Message::setBody(const std::string &body, std::string *error)
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

bool MessageBroker::Message::setBody(const JsonNode* json_node, std::string *error)
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

std::string MessageBroker::Message::serialize() const
{
	return _serialize(m_reqid, m_type, m_body);
}

std::string MessageBroker::Response::serialize() const
{
	return _serialize(m_reqid, m_type, m_body, m_reason);
}

std::string MessageBroker::Message::serializeBody() const
{
	g_autoptr(JsonGenerator) gen = json_generator_new();
	json_generator_set_root(gen, m_body);

	return std::string(json_generator_to_data(gen, NULL));
}

void MessageBroker::Response::setReason(const std::string &reason)
{
	m_type = reason.empty() ? "response" : "error";
	m_reason = reason;
}
