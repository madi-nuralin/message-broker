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
#include "common.h"

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

MessageBroker::MessageBroker(
	const std::string &host, int port,
	const std::string &username,
	const std::string &password,
	const std::string &vhost, int frame_max) {
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

	/*m_host = host;
	m_port = port;
	m_username = username;
	m_password = password;
	m_vhost = vhost;
	m_frame_max = frame_max;*/

	connection = new Connection(host, port, username, password, vhost, frame_max);
}

MessageBroker::~MessageBroker()
{
	delete connection;
}

void MessageBroker::publish(const Configuration configuration, const std::string &messagebody)
{
	Channel channel(connection);

	channel.setup_queue(
		configuration.queue.name,
		configuration.exchange.name,
		configuration.routing_key,
		configuration.queue.passive,
		configuration.queue.auto_delete,
		configuration.queue.exclusive
		);

	Message request;
	request.setBody(messagebody);

	::Message message(request.serialize());
	message.setProperty("Content-Type", "application/json");
	message.setProperty("Delivery-Mode", (uint8_t)2);
	
	channel.publish(configuration.exchange.name, configuration.routing_key, message);
}

void MessageBroker::publish(const Configuration configuration, const std::string &messagebody, std::function<void (const Response&)> callback)
{
	std::thread worker([=](){
		Channel channel(connection);

		auto reply_to = channel.setup_queue(
			configuration.queue.name,
			configuration.exchange.name,
			configuration.binding_key,
			configuration.queue.passive,
			configuration.queue.auto_delete,
			configuration.queue.exclusive
			);

		Request request;
		request.setBody(messagebody);

		::Message message(request.serialize());
		message.setProperty("Content-Type", "application/json");
		message.setProperty("Correlation-Id", request.reqid().c_str());
		message.setProperty("Delivery-Mode", (uint8_t)2);
		message.setProperty("Reply-To", reply_to.c_str());
	
		channel.publish(configuration.exchange.name, configuration.routing_key, message);

		channel.consume(configuration.queue.name, [&](auto& channel, const auto& envelope) {
			callback(Response(std::string((char*)envelope.message.body.bytes, envelope.message.body.len)));
		});
	});

	worker.detach();
}

void MessageBroker::subscribe(const Configuration configuration, std::function<void (const Message&)> callback)
{
	std::thread worker([=](){
		Channel channel(connection);

		channel.setup_queue(
			configuration.queue.name,
			configuration.exchange.name,
			configuration.binding_key,
			configuration.queue.passive,
			configuration.queue.auto_delete,
			configuration.queue.exclusive
			);

		channel.consume(configuration.queue.name, [&](auto& channel, const auto& envelope) {
			callback(Message(std::string((char*)envelope.message.body.bytes, envelope.message.body.len)));
		});
	});

	worker.detach();
}

void MessageBroker::subscribe(const Configuration configuration, std::function<void (const Request&, Response&)> callback)
{
	std::thread worker([=](){
		Channel channel(connection);

		channel.setup_queue(
			configuration.queue.name,
			configuration.exchange.name,
			configuration.binding_key,
			configuration.queue.passive,
			configuration.queue.auto_delete,
			configuration.queue.exclusive
			);

		channel.consume(configuration.queue.name, [&callback](auto& channel, const auto& envelope){
			MessageBroker::Request request(std::string((char*)envelope.message.body.bytes, envelope.message.body.len));
			MessageBroker::Response response(request);

			callback(request, response);

			std::string reply_to((char*)envelope.message.properties.reply_to.bytes, envelope.message.properties.reply_to.len);
			std::string correlation_id((char*)envelope.message.properties.correlation_id.bytes, envelope.message.properties.correlation_id.len);

			::Message message(response.serialize());
			message.setProperty("Content-Type", "application/json");
			message.setProperty("Correlation-Id", correlation_id.c_str());

			channel.publish("", reply_to, message);
		});
	});

	worker.detach();
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

MessageBroker::Message::Message(const std::string &str)
	: MessageBroker::Message::Message()
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

MessageBroker::Response::Response(const std::string &str)
	: MessageBroker::Message::Message(str)
{
	g_autoptr(JsonParser) parser = json_parser_new();
	json_parser_load_from_data(parser, (gchar*)str.c_str(), -1, NULL);

	g_autoptr(JsonReader) reader = json_reader_new(json_parser_get_root(parser));

	json_reader_read_member(reader, "reason");
	m_reason = json_reader_get_string_value(reader);
	json_reader_end_member(reader);
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

} // end namespace gammasoft