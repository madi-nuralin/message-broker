#include <stdio.h>
#include <assert.h>
#include <glib.h>
#include <set>
#include <chrono>
#include <thread>
#include <limits>
#include <stdexcept>
#include <iostream>

#include "message_broker.hpp"
#include "utils.h"

namespace gammasoft {

namespace amqp {

IdManager::IdManager()
{
    free_.insert(id_interval(1, std::numeric_limits<int>::max()));
}

int IdManager::allocateId()
{
    id_interval first = *(free_.begin());
    int free_id = first.left();
    free_.erase(free_.begin());
    if (first.left() + 1 <= first.right()) {
        free_.insert(id_interval(first.left() + 1 , first.right()));
    }
    return free_id;
}

bool IdManager::markAsUsed(int id)
{
    id_intervals_t::iterator it = free_.find(id_interval(id,id));
    if (it == free_.end()) {
        return false;
    } else {
        id_interval free_interval = *(it);
        free_.erase (it);
        if (free_interval.left() < id) {
            free_.insert(id_interval(free_interval.left(), id-1));
        }
        if (id +1 <= free_interval.right() ) {
            free_.insert(id_interval(id+1, free_interval.right()));
        }
        return true;
    }
}

void IdManager::freeId(int id)
{
    id_intervals_t::iterator it = free_.find(id_interval(id,id));
    if (it != free_.end()  && it->left() <= id && it->right() > id) {
        return ;
    }
    it = free_.upper_bound(id_interval(id,id));
    if (it == free_.end()) {
        return ;
    } else {
        id_interval free_interval = *(it);

        if (id + 1 != free_interval.left()) {
            free_.insert(id_interval(id, id));
        } else {
            if (it != free_.begin()) {
                id_intervals_t::iterator it_2 = it;
                --it_2;
                if (it_2->right() + 1 == id ) {
                    id_interval free_interval_2 = *(it_2);
                    free_.erase(it);
                    free_.erase(it_2);
                    free_.insert(
                        id_interval(free_interval_2.left(), 
                                    free_interval.right()));
                } else {
                    free_.erase(it);
                    free_.insert(id_interval(id, free_interval.right()));
                }
            } else {
                    free_.erase(it);
                    free_.insert(id_interval(id, free_interval.right()));
            }
        }
    }
}

bool id_interval::operator < (const id_interval& s) const
{
    return (left_ < s.left_) && (right_ < s.right_);
}

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

	std::thread worker([this]() {
		while (listen) {
			std::unique_lock<std::mutex> lock(this->lock);
			if (!pool.empty()) {
				amqp_rpc_reply_t res;
				amqp_envelope_t envelope;

				amqp_maybe_release_buffers(state);

				struct timeval tv = {.tv_sec = 0, .tv_usec = 100};

				res = amqp_consume_message(state, &envelope, &tv, 0);

				if (AMQP_RESPONSE_NORMAL != res.reply_type) {
					continue;
				}

				pool[envelope.channel]->envelopes.push(envelope);
			}
		}
	});

	worker.detach();
}

Connection::~Connection()
{
	std::unique_lock<std::mutex> lock(this->lock);
	listen = false;
	pool.clear();
	die_on_amqp_error(amqp_connection_close(state, AMQP_REPLY_SUCCESS),
			"Closing connection");
	die_on_error(amqp_destroy_connection(state), "Ending connection");
}

Channel::Channel(Connection *connection)
{
	std::unique_lock<std::mutex> lock(connection->lock);
	this->id = connection->manager.allocateId();
	this->connection = connection;
	amqp_channel_open(connection->state, id);
	die_on_amqp_error(amqp_get_rpc_reply(connection->state),
			"Opening channel");
}

Channel::~Channel()
{
	std::unique_lock<std::mutex> lock(connection->lock);
	die_on_amqp_error(
		amqp_channel_close(connection->state, id, AMQP_REPLY_SUCCESS),
			"Closing channel");
	connection->manager.freeId(id);
}

std::tuple<std::string, std::string> Channel::setup(const Configuration& configuration)
{
	std::unique_lock<std::mutex> lock(connection->lock);
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
	std::unique_lock<std::mutex> lock(connection->lock);

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
		"Publishing");
}

void Channel::consume(const std::string &queue_name, struct timeval *timeout, std::function<void(Channel &, const Envelope &)> callback, const std::string &consumer_tag, bool no_local, bool no_ack, bool exclusive)
{
	{
		std::unique_lock<std::mutex> lock(connection->lock);

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

		connection->pool[id] = this;
		std::cout << "Awaiting requests on channel " << id << std::endl;
	}

	std::chrono::time_point start = std::chrono::steady_clock::now();
	is_basic_cancel = false;
	while (!is_basic_cancel) {
		while (!envelopes.empty()) {
			auto envelope = Envelope(envelopes.front());
			envelopes.pop();
			callback(*this, envelope);
			start = std::chrono::steady_clock::now();
		}

		if (timeout) {
			if (std::chrono::steady_clock::now() - start > 
				std::chrono::milliseconds(timeout->tv_sec*1000 + timeout->tv_usec)) {
				throw std::runtime_error("AMQP consumer not responding, connection timed out");
			}
		}
	}

	connection->pool.erase(id);
	std::cout << "Stop listening requests on channel " << id << std::endl;
}

void Channel::cancel(const std::string &consumer_tag)
{
	std::unique_lock<std::mutex> lock(connection->lock);
	is_basic_cancel = true;
	if (!consumer_tag.empty()) {
		amqp_basic_cancel(connection->state, id, amqp_cstring_bytes(consumer_tag.c_str()));
		die_on_amqp_error(amqp_get_rpc_reply(connection->state), "basic.cancel");
	}
}

void Channel::qos(uint32_t prefetch_size, uint16_t prefetch_count, bool global)
{
	std::unique_lock<std::mutex> lock(connection->lock);
	if (!amqp_basic_qos(connection->state, id, prefetch_count, prefetch_size, global)) {
		die_on_amqp_error(amqp_get_rpc_reply(connection->state), "basic.qos");
	}
}

void Channel::acknowledge(uint64_t delivery_tag, bool multiple)
{
	std::unique_lock<std::mutex> lock(connection->lock);
	die_on_error(amqp_basic_ack(connection->state, id, delivery_tag, multiple), "basic.ack");
}

void Channel::reject(uint64_t delivery_tag, bool multiple, bool requeue)
{
	std::unique_lock<std::mutex> lock(connection->lock);
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

	m_connection = std::make_shared<Connection>(host, port, username, password, vhost, frame_max);
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

	m_connection = std::make_shared<Connection>(ci.host,ci.port,ci.user,ci.password,ci.vhost,frame_max);

	free(p);
}

MessageBroker::~MessageBroker()
{
}

void MessageBroker::publish(const Configuration &configuration, const std::string &messagebody)
{
	Channel channel(m_connection.get());

	auto[exchange, queue] = channel.setup(configuration);

	Message message;
	message.setBody(messagebody);
	message.setProperty("Content-Type", "application/json");
	message.setProperty("Delivery-Mode", (uint8_t)2);
	
	channel.publish(exchange, configuration.routing_key, message);
}

void MessageBroker::publish(const Configuration &configuration, const std::string &messagebody, std::function<void (const Response&)> callback)
{
	std::thread worker([=](){
		try {
			Channel channel(m_connection.get());

			auto[exchange, reply_to] = channel.setup(configuration);

			Request request;
			request.setBody(messagebody);
			request.setProperty("Content-Type", "application/json");
			request.setProperty("Correlation-Id", generateReqId().c_str());
			request.setProperty("Delivery-Mode", (uint8_t)2);
			request.setProperty("Reply-To", reply_to.c_str());
			request.setProperty("Type", "request");

			channel.publish(exchange, configuration.routing_key, request);
			channel.consume(reply_to, nullptr, [&](auto& channel, const auto& envelope) {
				callback(envelope.message);
				channel.cancel(std::string((char *)envelope.consumer_tag.bytes, envelope.consumer_tag.len));
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

MessageBroker::Response::Ptr MessageBroker::publish(const Configuration &configuration, const std::string &messagebody, struct timeval *timeout)
{
	Channel channel(m_connection.get());

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
	channel.consume(reply_to, timeout, [&](auto& channel, const auto& envelope){
		response = std::make_shared<Response>(envelope.message);
		channel.cancel(std::string((char *)envelope.consumer_tag.bytes, envelope.consumer_tag.len));
	});

	return response;
}

void MessageBroker::subscribe(const Configuration &configuration, std::function<void (const Message&)> callback)
{
	std::thread worker([=](){
		try {
			Channel channel(m_connection.get());

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
