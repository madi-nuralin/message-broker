#include <stdio.h>
#include <assert.h>
#include <glib.h>
#include <stdexcept>
#include <iostream>

#include "message_broker.hpp"
#include "utils.h"

namespace gammasoft {

namespace amqp {

Connection::Connection(
	const std::string &host, int port,
	const std::string &user,
	const std::string &password,
	const std::string &vhost, int frame_max
	)
{
	state = amqp_new_connection();
	
	amqp_socket_t *socket = amqp_tcp_socket_new(state);
	if (!socket) {
		die("creating TCP socket failed");
	}

	int status = amqp_socket_open(socket, host.c_str(), port);
	if (status) {
		die("opening TCP socket on %s:%d failed", host.c_str(), port);
	}

	die_on_amqp_error(amqp_login(state, vhost.c_str(), 0, frame_max, 0, AMQP_SASL_METHOD_PLAIN,
			user.c_str(), password.c_str()),
		"Logging in");
}

Connection::~Connection()
{
	die_on_amqp_error(amqp_connection_close(state, AMQP_REPLY_SUCCESS),
			"Closing connection");
	die_on_error(amqp_destroy_connection(state), "Ending connection");
}

Channel::Channel(Connection *connection) : id(1), connection(connection)
{
	amqp_channel_open(connection->state, id);
	die_on_amqp_error(amqp_get_rpc_reply(connection->state),
			"Opening channel");
}

Channel::~Channel()
{
	die_on_amqp_error(
		amqp_channel_close(connection->state, id, AMQP_REPLY_SUCCESS),
			"Closing channel");
}

std::tuple<std::string, std::string> Channel::setup(const Configuration& configuration)
{
	std::string exchange, queue;

	if (configuration.exchange.name == "amq") {
		exchange = "amq." + configuration.exchange.type;
	}

	if (configuration.exchange.declare) {
		amqp_exchange_declare(
			connection->state,
			id,
			configuration.exchange.name.empty()
				? amqp_empty_bytes
				: amqp_cstring_bytes(configuration.exchange.name.c_str()),
			configuration.exchange.type.empty()
				? amqp_empty_bytes
				: amqp_cstring_bytes(configuration.exchange.type.c_str()),
			configuration.exchange.passive,
			configuration.exchange.durable,
			configuration.exchange.auto_delete,
			configuration.exchange.internal,
			amqp_empty_table);

		die_on_amqp_error(
			amqp_get_rpc_reply(connection->state), "Declaring exchange");

		exchange = configuration.exchange.name;
	}

	if (configuration.queue.declare) {
		amqp_queue_declare_ok_t *r =
			amqp_queue_declare(
				connection->state,
				id,
				configuration.queue.name.empty()
					? amqp_empty_bytes
					: amqp_cstring_bytes(configuration.queue.name.c_str()),
				configuration.queue.passive,
				configuration.queue.durable,
				configuration.queue.exclusive,
				configuration.queue.auto_delete,
				amqp_empty_table);

		die_on_amqp_error(
			amqp_get_rpc_reply(connection->state), "Declaring queue");

		queue = std::string((char*)r->queue.bytes, r->queue.len);
	}

	if (configuration.queue.bind) {
		amqp_queue_bind(
			connection->state,
			id,
			queue.empty()
				? amqp_empty_bytes
				: amqp_cstring_bytes(queue.c_str()),
			exchange.empty()
				? amqp_empty_bytes
				: amqp_cstring_bytes(exchange.c_str()),
			configuration.routing_key.empty()
				? amqp_empty_bytes
				: amqp_cstring_bytes(configuration.routing_key.c_str()),
			amqp_empty_table);

		die_on_amqp_error(
			amqp_get_rpc_reply(connection->state), "Binding queue");
	}

	return std::make_tuple(exchange, queue);
}

void Channel::publish(const std::string &exchange, const std::string &routing_key, const Message &message, bool mandatory, bool immediate)
{
	die_on_error(
		amqp_basic_publish(
			connection->state,
			id,
			exchange.empty()
				? amqp_empty_bytes
				: amqp_cstring_bytes(exchange.c_str()),
			routing_key.empty()
				? amqp_empty_bytes
				: amqp_cstring_bytes(routing_key.c_str()),
			mandatory,
			immediate,
			&message.properties,
			message.body),
		"basic.publish");
}

void Channel::consume(const std::string &queue_name, struct timeval *timeout, std::function<void(const Envelope &)> callback, const std::string &consumer_tag, bool no_local, bool no_ack, bool exclusive)
{
	amqp_basic_consume(
		connection->state,
		id,
		queue_name.empty()
			? amqp_empty_bytes
			: amqp_cstring_bytes(queue_name.c_str()),
		consumer_tag.empty()
			? amqp_empty_bytes
			: amqp_cstring_bytes(consumer_tag.c_str()),
		no_local,
		no_ack,
		exclusive,
		amqp_empty_table);

	die_on_amqp_error(
		amqp_get_rpc_reply(connection->state), "Consuming");

	for (;;) {
		amqp_rpc_reply_t res;
		amqp_envelope_t envelope;

		amqp_maybe_release_buffers(state);

		res = amqp_consume_message(state, &envelope, &tv, 0);

		if (AMQP_RESPONSE_NORMAL != res.reply_type) {
			continue;
		}

		callback(envelope);
	}
}

void Channel::cancel(const std::string &consumer_tag)
{
	if (!consumer_tag.empty()) {
		amqp_basic_cancel(connection->state, id, amqp_cstring_bytes(consumer_tag.c_str()));
		die_on_amqp_error(amqp_get_rpc_reply(connection->state), "basic.cancel");
	}
}

void Channel::qos(uint32_t prefetch_size, uint16_t prefetch_count, bool global)
{
	if (!amqp_basic_qos(connection->state, id, prefetch_count, prefetch_size, global)) {
		die_on_amqp_error(amqp_get_rpc_reply(connection->state), "basic.qos");
	}
}

void Channel::acknowledge(uint64_t delivery_tag, bool multiple)
{
	die_on_error(amqp_basic_ack(connection->state, id, delivery_tag, multiple), "basic.ack");
}

void Channel::reject(uint64_t delivery_tag, bool multiple, bool requeue)
{
	die_on_error(amqp_basic_nack(connection->state, id, delivery_tag, multiple, requeue), "basic.nack");
}

} // end namespace amqp

using namespace gammasoft::amqp;

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

MessageBroker::MessageBroker(
	const std::string &host, int port,
	const std::string &username,
	const std::string &password,
	const std::string &vhost, int frame_max
	)
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

MessageBroker::MessageBroker(
	const std::string &url, int frame_max
	)
{
	std::srand(std::time(NULL));

	struct amqp_connection_info ci;
	char *p = strdup(url.c_str());

	die_on_error(
		amqp_parse_url(p, &ci), "Parse URL");

	m_host = ci.host;
	m_port = ci.port;
	m_username = ci.user;
	m_password = ci.password;
	m_vhost = ci.vhost;
	m_frame_max = frame_max;

	free(p);
}

void MessageBroker::publish(const Configuration &configuration, const std::string &messagebody)
{
	Connection connection(m_host,m_port,m_username,m_password,m_vhost,m_frame_max);
	Channel channel(&connection);

	auto[exchange, queue] = channel.setup(configuration);

	Message message;
	message.setBody(messagebody);
	message.setProperty("Content-Type", "application/json");
	message.setProperty("Delivery-Mode", (uint8_t)2);
	
	channel.publish(exchange, configuration.routing_key, message);
}

MessageBroker::Response::Ptr MessageBroker::publish(const Configuration &configuration, const std::string &messagebody, struct timeval *timeout)
{
	Connection connection(m_host,m_port,m_username,m_password,m_vhost,m_frame_max);
	Channel channel(&connection);

	auto[exchange, reply_to] = channel.setup(configuration);

	Request request;
	request.setBody(messagebody);
	request.setProperty("Content-Type", "application/json");
	request.setProperty("Correlation-Id", generateReqId().c_str());
	request.setProperty("Delivery-Mode", (uint8_t)2);
	request.setProperty("Reply-To", reply_to.c_str());
	request.setProperty("Type", "request");

	Response::Ptr response;

	channel.publish(exchange, configuration.routing_key, request);
	channel.consume(reply_to, timeout, [&](const auto& envelope){
		response = std::make_shared<Response>(envelope.message);
		channel.close();
	});

	return response;
}

void MessageBroker::subscribe(const Configuration &configuration, std::function<void (const Message&)> callback)
{
	std::thread worker([=](){
		try {
			Connection connection(m_host,m_port,m_username,m_password,m_vhost,m_frame_max);
			Channel channel(&connection);

			auto[exchange, queue] = channel.setup(configuration);

			channel.consume(queue, nullptr, [&](auto& channel, const auto& envelope) {
				callback(envelope.message);
			});
		} catch (const std::runtime_error& e) {
			if (configuration.on_error) {
				configuration.on_error(e.what());
				return;
			}
			std::cout << e.what() << std::endl;
		}
	});

	worker.detach();
}

void MessageBroker::subscribe(const Configuration &configuration, std::function<bool (const Request&, Response&)> callback)
{
	std::thread worker([=](){
		try {
			Connection connection(m_host,m_port,m_username,m_password,m_vhost,m_frame_max);
			Channel channel(m_connection.get());

			auto[exchange, queue] = channel.setup(configuration);

			channel.consume(queue, nullptr, [&callback](auto& channel, const auto& envelope){
				Request request(envelope.message);
				Response response;

				auto ok = callback(request, response);

				std::string reply_to((char*)envelope.message.properties.reply_to.bytes, envelope.message.properties.reply_to.len);
				std::string correlation_id((char*)envelope.message.properties.correlation_id.bytes, envelope.message.properties.correlation_id.len);

				response.setProperty("Content-Type", "application/json");
				response.setProperty("Correlation-Id", correlation_id.c_str());
				response.setProperty("Delivery-Mode", (uint8_t)2);
				response.setProperty("Type", ok ? "response" : "error");

				channel.publish("", reply_to, response);
			});
		} catch (const std::runtime_error &e) {
			if (configuration.on_error) {
				configuration.on_error(e.what());
				return;
			}
			std::cout << e.what() << std::endl;
		}
	});

	worker.detach();
}

} // end namespace gammasoft
