#include <stdio.h>
#include <assert.h>
#include <glib.h>
#include <stdexcept>
#include <iostream>
#include <thread>

#include "message_broker.hpp"
#include "utils.h"

namespace gammasoft {

namespace amqp {

class ConnectionImpl
{
	amqp_connection_state_t m_state;

public:
	inline ConnectionImpl(
		const std::string &host = "127.0.0.1", int port = 5672,
		const std::string &username = "guest",
		const std::string &password = "guest",
		const std::string &vhost = "/", int frame_max = 131072)
	{
		m_state = amqp_new_connection();
		amqp_socket_t *socket = amqp_tcp_socket_new(m_state);
		if (!socket) {
			die("AMQP creating TCP socket failed");
		}
		int status = amqp_socket_open(socket, host.c_str(), port);
		if (status) {
			die("AMQP opening TCP socket on %s:%d failed", host.c_str(), port);
		}
		die_on_amqp_error(amqp_login(m_state, vhost.c_str(), 0, frame_max, 0, AMQP_SASL_METHOD_PLAIN,
				username.c_str(), password.c_str()),
			"Logging in");
	}

	inline virtual ~ConnectionImpl()
	{
		die_on_amqp_error(amqp_connection_close(m_state, AMQP_REPLY_SUCCESS),
			"connection.close");
		die_on_error(amqp_destroy_connection(m_state),
			"destroy.connection");
	}

	void maybe_release_buffers()
	{
		amqp_maybe_release_buffers(m_state);
	}

	RpcReply consume_message(Envelope *envelope, const struct timeval *timeout, int flags)
	{
		return amqp_consume_message(m_state, envelope, timeout, flags);
	}

	friend class ChannelImpl;
};

class ChannelImpl
{
	amqp_channel_t m_id;
	ConnectionImpl *m_connection;

public:
	inline ChannelImpl(Connection *connection) : m_id(1)
	{
		m_connection = connection->m_impl.get();
		amqp_channel_open(m_connection->m_state, m_id);
		die_on_amqp_error(amqp_get_rpc_reply(m_connection->m_state),
			"channel.open");
	}

	inline virtual ~ChannelImpl()
	{
		die_on_amqp_error(amqp_channel_close(m_connection->m_state, m_id, AMQP_REPLY_SUCCESS),
			"channel.close");
	}

	std::tuple<std::string, std::string> setup(const Configuration& configuration)
	{
		std::string exchange, queue;

		if (configuration.exchange.name == "amq") {
			exchange = "amq." + configuration.exchange.type;
		}

		if (configuration.exchange.declare) {
			amqp_exchange_declare(
				m_connection->m_state,
				m_id,
				amqp_cstring_bytes(configuration.exchange.name.c_str()),
				amqp_cstring_bytes(configuration.exchange.type.c_str()),
				configuration.exchange.passive,
				configuration.exchange.durable,
				configuration.exchange.auto_delete,
				configuration.exchange.internal,
				amqp_empty_table);

			die_on_amqp_error(
				amqp_get_rpc_reply(m_connection->m_state), "exchange.declare");

			exchange = configuration.exchange.name;
		}

		if (configuration.queue.declare) {
			amqp_queue_declare_ok_t *r =
				amqp_queue_declare(
					m_connection->m_state,
					m_id,
					amqp_cstring_bytes(configuration.queue.name.c_str()),
					configuration.queue.passive,
					configuration.queue.durable,
					configuration.queue.exclusive,
					configuration.queue.auto_delete,
					amqp_empty_table);

			die_on_amqp_error(
				amqp_get_rpc_reply(m_connection->m_state), "queue.declare");

			queue = std::string((char*)r->queue.bytes, r->queue.len);
		}

		if (configuration.queue.bind) {
			amqp_queue_bind(
				m_connection->m_state,
				m_id,
				amqp_cstring_bytes(queue.c_str()),
				amqp_cstring_bytes(exchange.c_str()),
				amqp_cstring_bytes(configuration.routing_key.c_str()),
				amqp_empty_table);

			die_on_amqp_error(
				amqp_get_rpc_reply(m_connection->m_state), "queue.bind");
		}

		return std::make_tuple(exchange, queue);
	}

	void basic_publish(const std::string &exchange, const std::string &routing_key, const Message &message, bool mandatory, bool immediate)
	{
		die_on_error(
			amqp_basic_publish(
				m_connection->m_state,
				m_id,
				amqp_cstring_bytes(exchange.c_str()),
				amqp_cstring_bytes(routing_key.c_str()),
				mandatory,
				immediate,
				&message.properties,
				message.body),
			"basic.publish");
	}

	void basic_consume(const std::string &queue_name, const std::string &consumer_tag, bool no_local, bool no_ack, bool exclusive)
	{
		amqp_basic_consume(
			m_connection->m_state,
			m_id,
			amqp_cstring_bytes(queue_name.c_str()),
			amqp_cstring_bytes(consumer_tag.c_str()),
			no_local,
			no_ack,
			exclusive,
			amqp_empty_table);

		die_on_amqp_error(
			amqp_get_rpc_reply(m_connection->m_state), "basic.consume");
	}

	void basic_cancel(const std::string &consumer_tag)
	{
		if (!consumer_tag.empty()) {
			amqp_basic_cancel(m_connection->m_state, m_id, amqp_cstring_bytes(consumer_tag.c_str()));
			die_on_amqp_error(amqp_get_rpc_reply(m_connection->m_state), "basic.cancel");
		}
	}

	void basic_qos(uint32_t prefetch_size, uint16_t prefetch_count, bool global)
	{
		if (!amqp_basic_qos(m_connection->m_state, m_id, prefetch_count, prefetch_size, global)) {
			die_on_amqp_error(amqp_get_rpc_reply(m_connection->m_state), "basic.qos");
		}
	}

	void basic_ack(uint64_t delivery_tag, bool multiple)
	{
		die_on_error(amqp_basic_ack(m_connection->m_state, m_id, delivery_tag, multiple), "basic.ack");
	}

	void basic_nack(uint64_t delivery_tag, bool multiple, bool requeue)
	{
		die_on_error(amqp_basic_nack(m_connection->m_state, m_id, delivery_tag, multiple, requeue), "basic.nack");
	}
};

Connection::Connection(
	const std::string &host, int port,
	const std::string &user,
	const std::string &password,
	const std::string &vhost, int frame_max
	) : m_impl(new ConnectionImpl(host, port, user, password, vhost, frame_max))
{
}

Connection::~Connection()
{
}

void Connection::maybe_release_buffers()
{
	m_impl->maybe_release_buffers();
}

RpcReply Connection::consume_message(Envelope *envelope, const struct timeval *timeout, int flags)
{
	return m_impl->consume_message(envelope, timeout, flags);
}

Channel::Channel(Connection *connection)
	: m_impl(new ChannelImpl(connection))
{
}

Channel::~Channel()
{
}
	
std::tuple<std::string,std::string> Channel::setup(const Configuration &configration)
{
	return m_impl->setup(configration);
}

void Channel::basic_publish(const std::string &exchange, const std::string &routing_key, const Message &message, bool mandatory, bool immediate)
{
	m_impl->basic_publish(exchange, routing_key, message, mandatory, immediate);
}

void Channel::basic_consume(const std::string &queue_name, const std::string &consumer_tag, bool no_local, bool no_ack, bool exclusive)
{
	m_impl->basic_consume(queue_name, consumer_tag, no_local, no_ack, exclusive);
}

void Channel::basic_cancel(const std::string &consumer_tag)
{
	m_impl->basic_cancel(consumer_tag);
}

void Channel::basic_qos(uint32_t prefetch_size, uint16_t prefetch_count, bool global)
{
	m_impl->basic_qos(prefetch_size, prefetch_count, global);
}

void Channel::basic_ack(uint64_t delivery_tag, bool multiple)
{
	m_impl->basic_ack(delivery_tag, multiple);
}

void Channel::basic_nack(uint64_t delivery_tag, bool multiple, bool requeue)
{
	m_impl->basic_nack(delivery_tag, multiple, requeue);
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
		throw std::runtime_error("port is not valm_id, it must be a positive number");
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
		amqp_parse_url(p, &ci), "parse.url");

	m_host = ci.host;
	m_port = ci.port;
	m_username = ci.user;
	m_password = ci.password;
	m_vhost = ci.vhost;
	m_frame_max = frame_max;

	free(p);
}

MessageBroker::~MessageBroker()
{
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
	
	channel.basic_publish(exchange, configuration.routing_key, message);
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

	channel.basic_publish(exchange, configuration.routing_key, request);
	channel.basic_consume(reply_to);

	for(;;) {
		RpcReply res;
		Envelope envelope;

		connection.maybe_release_buffers();
		res = connection.consume_message(&envelope, timeout, 0);

		if (AMQP_RESPONSE_NORMAL != res.reply_type) {
			continue;
		}

		response = std::make_shared<Response>(envelope.message);

		amqp_destroy_envelope(&envelope);

		break;
	}

	return response;
}

void MessageBroker::subscribe(const Configuration &configuration, std::function<void (const Message&)> callback)
{
	std::thread worker([this, configuration, callback](){
		Connection connection(m_host,m_port,m_username,m_password,m_vhost,m_frame_max);
		Channel channel(&connection);

		auto[exchange, queue] = channel.setup(configuration);

		channel.basic_consume(queue);

		for(;;) {
			RpcReply res;
			Envelope envelope;

			connection.maybe_release_buffers();
			res = connection.consume_message(&envelope, nullptr, 0);

			if (AMQP_RESPONSE_NORMAL != res.reply_type) {
				continue;
			}

			callback(envelope.message);

			amqp_destroy_envelope(&envelope);
		}
	});

	worker.detach();
}

void MessageBroker::subscribe(const Configuration &configuration, std::function<bool (const Request&, Response&)> callback)
{
	std::thread worker([this, configuration, callback](){
		Connection connection(m_host,m_port,m_username,m_password,m_vhost,m_frame_max);
		Channel channel(&connection);

		auto[exchange, queue] = channel.setup(configuration);

		channel.basic_consume(queue);

		for(;;) {
			RpcReply res;
			Envelope envelope;

			connection.maybe_release_buffers();
			res = connection.consume_message(&envelope, nullptr, 0);

			if (AMQP_RESPONSE_NORMAL != res.reply_type) {
				continue;
			}

			Request request(envelope.message);
			Response response;

			auto ok = callback(request, response);

			std::string reply_to((char*)envelope.message.properties.reply_to.bytes, envelope.message.properties.reply_to.len);
			std::string correlation_id((char*)envelope.message.properties.correlation_id.bytes, envelope.message.properties.correlation_id.len);

			response.setProperty("Content-Type", "application/json");
			response.setProperty("Correlation-Id", correlation_id.c_str());
			response.setProperty("Delivery-Mode", (uint8_t)2);
			response.setProperty("Type", ok ? "response" : "error");

			channel.basic_publish("", reply_to, response);

			amqp_destroy_envelope(&envelope);
		}
	});

	worker.detach();
}

} // end namespace gammasoft
