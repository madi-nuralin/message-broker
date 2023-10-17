#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdexcept>
#include <set>
#include <thread>
#include <chrono>

#include "message_broker.hpp"
#include "utils.h"

namespace soft {

namespace amqp {

class id_interval 
{
public:
	id_interval(int ll, int uu) : m_left(ll), m_right(uu) {}
	bool operator < (const id_interval& ) const;
	int left() const { return m_left; }
	int right() const {  return m_right; }
private:
	int m_left, m_right;
};

class IdManager {
public:
	IdManager();
	int allocateId();          // Allocates an id
	void freeId(int id);       // Frees an id so it can be used again
	bool markAsUsed(int id);   // Let's the user register an id. 
private: 
	typedef std::set<id_interval> id_intervals_t;
	id_intervals_t m_free;
};

IdManager::IdManager()
{
    m_free.insert(id_interval(1, std::numeric_limits<int>::max()));
}

int IdManager::allocateId()
{
    id_interval first = *(m_free.begin());
    int free_id = first.left();
    m_free.erase(m_free.begin());
    if (first.left() + 1 <= first.right()) {
        m_free.insert(id_interval(first.left() + 1 , first.right()));
    }
    return free_id;
}

bool IdManager::markAsUsed(int id)
{
    id_intervals_t::iterator it = m_free.find(id_interval(id,id));
    if (it == m_free.end()) {
        return false;
    } else {
        id_interval free_interval = *(it);
        m_free.erase (it);
        if (free_interval.left() < id) {
            m_free.insert(id_interval(free_interval.left(), id-1));
        }
        if (id +1 <= free_interval.right() ) {
            m_free.insert(id_interval(id+1, free_interval.right()));
        }
        return true;
    }
}

void IdManager::freeId(int id)
{
    id_intervals_t::iterator it = m_free.find(id_interval(id,id));
    if (it != m_free.end()  && it->left() <= id && it->right() > id) {
        return ;
    }
    it = m_free.upper_bound(id_interval(id,id));
    if (it == m_free.end()) {
        return ;
    } else {
        id_interval free_interval = *(it);

        if (id + 1 != free_interval.left()) {
            m_free.insert(id_interval(id, id));
        } else {
            if (it != m_free.begin()) {
                id_intervals_t::iterator it_2 = it;
                --it_2;
                if (it_2->right() + 1 == id ) {
                    id_interval free_interval_2 = *(it_2);
                    m_free.erase(it);
                    m_free.erase(it_2);
                    m_free.insert(
                        id_interval(free_interval_2.left(), 
                                    free_interval.right()));
                } else {
                    m_free.erase(it);
                    m_free.insert(id_interval(id, free_interval.right()));
                }
            } else {
                    m_free.erase(it);
                    m_free.insert(id_interval(id, free_interval.right()));
            }
        }
    }
}

bool id_interval::operator < (const id_interval& s) const
{
    return (m_left < s.m_left) && (m_right < s.m_right);
}

class ConnectionImpl
{
//	std::mutex m_lock;
//	IdManager m_idManager;
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
				"Closing connection");
		die_on_error(amqp_destroy_connection(m_state), "Ending connection");
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
	ConnectionImpl *m_connectionImpl;

public:
	inline ChannelImpl(Connection *connection)
		: m_connectionImpl(connection->m_impl.get()), m_id(1)
	{
		amqp_channel_open(m_connectionImpl->m_state, m_id);
		die_on_amqp_error(amqp_get_rpc_reply(m_connectionImpl->m_state),
				"Opening channel");
	}

	inline virtual ~ChannelImpl()
	{
		die_on_amqp_error(amqp_channel_close(m_connectionImpl->m_state, m_id, AMQP_REPLY_SUCCESS),
				"Closing connection");
	}

	std::tuple<std::string, std::string> setup(const Configuration& configuration)
	{
		std::string exchange, queue;

		if (configuration.exchange.name == "amq") {
			exchange = "amq." + configuration.exchange.type;
		}

		if (configuration.exchange.declare) {
			amqp_exchange_declare(
				m_connectionImpl->m_state,
				m_id,
				amqp_cstring_bytes(configuration.exchange.name.c_str()),
				amqp_cstring_bytes(configuration.exchange.type.c_str()),
				configuration.exchange.passive,
				configuration.exchange.durable,
				configuration.exchange.auto_delete,
				configuration.exchange.internal,
				amqp_empty_table);

			die_on_amqp_error(
				amqp_get_rpc_reply(m_connectionImpl->m_state), "exchange.declare");

			exchange = configuration.exchange.name;
		}

		if (configuration.queue.declare) {
			amqp_queue_declare_ok_t *r =
				amqp_queue_declare(
					m_connectionImpl->m_state,
					m_id,
					amqp_cstring_bytes(configuration.queue.name.c_str()),
					configuration.queue.passive,
					configuration.queue.durable,
					configuration.queue.exclusive,
					configuration.queue.auto_delete,
					amqp_empty_table);

			die_on_amqp_error(
				amqp_get_rpc_reply(m_connectionImpl->m_state), "queue.declare");

			queue = std::string((char*)r->queue.bytes, r->queue.len);
		}

		if (configuration.queue.bind) {
			amqp_queue_bind(
				m_connectionImpl->m_state,
				m_id,
				amqp_cstring_bytes(queue.c_str()),
				amqp_cstring_bytes(exchange.c_str()),
				amqp_cstring_bytes(configuration.routing_key.c_str()),
				amqp_empty_table);

			die_on_amqp_error(
				amqp_get_rpc_reply(m_connectionImpl->m_state), "queue.bind");
		}

		return std::make_tuple(exchange, queue);
	}

	void basic_publish(const std::string &exchange, const std::string &routing_key, const Message &message, bool mandatory, bool immediate)
	{
		die_on_error(
			amqp_basic_publish(
				m_connectionImpl->m_state,
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
			m_connectionImpl->m_state,
			m_id,
			amqp_cstring_bytes(queue_name.c_str()),
			amqp_cstring_bytes(consumer_tag.c_str()),
			no_local,
			no_ack,
			exclusive,
			amqp_empty_table);

		die_on_amqp_error(
			amqp_get_rpc_reply(m_connectionImpl->m_state), "basic.consume");
	}

	void basic_cancel(const std::string &consumer_tag)
	{
		if (!consumer_tag.empty()) {
			amqp_basic_cancel(m_connectionImpl->m_state, m_id, amqp_cstring_bytes(consumer_tag.c_str()));
			die_on_amqp_error(amqp_get_rpc_reply(m_connectionImpl->m_state), "basic.cancel");
		}
	}

	void basic_qos(uint32_t prefetch_size, uint16_t prefetch_count, bool global)
	{
		if (!amqp_basic_qos(m_connectionImpl->m_state, m_id, prefetch_count, prefetch_size, global)) {
			die_on_amqp_error(amqp_get_rpc_reply(m_connectionImpl->m_state), "basic.qos");
		}
	}

	void basic_ack(uint64_t delivery_tag, bool multiple)
	{
		die_on_error(amqp_basic_ack(m_connectionImpl->m_state, m_id, delivery_tag, multiple), "basic.ack");
	}

	void basic_nack(uint64_t delivery_tag, bool multiple, bool requeue)
	{
		die_on_error(amqp_basic_nack(m_connectionImpl->m_state, m_id, delivery_tag, multiple, requeue), "basic.nack");
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

using namespace soft::amqp;

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
		amqp_parse_url(p, &ci), "Parsing URL");

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
	m_close = true;
	for( auto it = m_threads.begin(); it != m_threads.end(); it++ ) {
		it->join();
	}
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

	struct timeval tv = {30,0};

	for(;;) {
		RpcReply res;
		Envelope envelope;

		connection.maybe_release_buffers();
		res = connection.consume_message(&envelope, timeout ? timeout : &tv, 0);

		if (AMQP_RESPONSE_NORMAL != res.reply_type) {
			die_on_error(res.library_error, "AMQP consume message");
		}

		response = std::make_shared<Response>(envelope.message);

		break;
	}

	return response;
}

void MessageBroker::subscribe(const Configuration &configuration, std::function<void (const Message&)> callback)
{
	std::thread worker([this, configuration, callback]()
	{
		struct timeval tv = {1,0};
		Connection connection(m_host,m_port,m_username,m_password,m_vhost,m_frame_max);
		Channel channel(&connection);

		auto[exchange, queue] = channel.setup(configuration);

		channel.basic_consume(queue);

		while(!m_close) {
			RpcReply res;
			Envelope envelope;

			std::this_thread::sleep_for(std::chrono::milliseconds(100));

			connection.maybe_release_buffers();
			res = connection.consume_message(&envelope, &tv, 0);

			if (AMQP_RESPONSE_NORMAL != res.reply_type) {
				continue;
			}

			callback(envelope.message);
		}
	});

	m_threads.push_back(std::move(worker));
}

void MessageBroker::subscribe(const Configuration &configuration, std::function<bool (const Request&, Response&)> callback)
{
	std::thread worker([this, configuration, callback]()
	{
		struct timeval tv = {1,0};
		Connection connection(m_host,m_port,m_username,m_password,m_vhost,m_frame_max);
		Channel channel(&connection);

		auto[exchange, queue] = channel.setup(configuration);

		channel.basic_consume(queue);

		while(!m_close) {
			RpcReply res;
			Envelope envelope;

			std::this_thread::sleep_for(std::chrono::milliseconds(100));

			connection.maybe_release_buffers();
			res = connection.consume_message(&envelope, &tv, 0);

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
		}
	});

	m_threads.push_back(std::move(worker));
}

void MessageBroker::close()
{
	m_close = true;
}

} // end namespace soft
