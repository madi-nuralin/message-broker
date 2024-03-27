#ifndef  MESSAGE_BROKER_H
#define  MESSAGE_BROKER_H

#include <map>
#include <string>
#include <utility>
#include <functional>
#include <memory>
#include <thread>
#include <vector>
#include <atomic>
#include <optional>

#include <rabbitmq-c/amqp.h>
#include <rabbitmq-c/tcp_socket.h>

namespace soft {

namespace amqp {
	struct Properties2 {
		std::optional<std::string> content_type;
		std::optional<std::string> content_encoding;
		std::optional<uint8_t> delivery_mode;
		std::optional<uint8_t> priority;
		std::optional<std::string> correlation_id;
		std::optional<std::string> reply_to;
		std::optional<std::string> expiration;
		std::optional<std::string> message_id;
		std::optional<uint64_t> timestamp;
		std::optional<std::string> type;
		std::optional<std::string> user_id;
		std::optional<std::string> app_id;
		std::optional<std::string> cluster_id;
	};

	struct Message2 {
		std::string body;
		Properties2 properties;
	};

	class Connection2 {
	public:
		using Ptr = std::shared_ptr<Connection2>;
        using WPtr = std::weak_ptr<Connection2>;

		Connection2();
		virtual ~Connection2();

		virtual void releaseBuffers() = 0;

		static Ptr createInstance();
	};

    class Channel2 {
    	///< `"direct"` string constant
        static const char* EXCHANGE_TYPE_DIRECT;

        ///< `"fanout"` string constant
        static const char* EXCHANGE_TYPE_FANOUT;

        ///< `"topic"` string constant
        static const char* EXCHANGE_TYPE_TOPIC;

    public:
        using Ptr = std::shared_ptr<Channel2>;
        using WPtr = std::weak_ptr<Channel2>;

        Channel2();
        virtual ~Channel2();

        /**
         * Declares an exchange
         *
         * Creates an exchange on the AMQP broker if it does not already exist
         * @param exchange_name the name of the exchange
         * @param exchange_type the type of exchange to be declared. Defaults to
         * `direct`; other types that could be used: `fanout`, `topic`
         * @param passive Indicates how the broker should react if the exchange does
         * not exist. If passive is `true` and the exhange does not exist the broker
         * will respond with an error and not create the exchange; exchange is created
         * otherwise. Defaults to `false` (exchange is created if needed)
         * @param durable Indicates whether the exchange is durable - e.g., will it
         * survive a broker restart.
         * @param auto_delete Indicates whether the exchange will automatically be
         * removed when no queues are bound to it.
         */
        virtual void exchangeDeclare(
            const std::string& exchange_name,
            const std::string& exchange_type = Channel2::EXCHANGE_TYPE_DIRECT,
            bool passive = false,
            bool durable = false,
            bool auto_delete = false,
            bool internal = false)
            = 0;

        /**
         * Binds one exchange to another exchange using a given key
         * @param destination the name of the exchange to route messages to
         * @param source the name of the exchange to route messages from
         * @param routing_key the routing key to use when binding
         */
        virtual void exchangeBind(
            const std::string& destination,
            const std::string& source,
            const std::string& routing_key)
            = 0;

        /**
         * Unbind an existing exchange-exchange binding
         * @see BindExchange
         * @param destination the name of the exchange to route messages to
         * @param source the name of the exchange to route messages from
         * @param routing_key the routing key to use when binding
         */
        virtual void exchangeUnbind(
            const std::string& destination,
            const std::string& source,
            const std::string& routing_key)
            = 0;

        /**
         * Declare a queue
         *
         * Creates a queue on the AMQP broker if it does not already exist.
         * @param queue_name The desired name of the queue. If this is an empty
         * string, the broker will generate a queue name that this method will return.
         * @param passive Indicated how the broker should react if the queue does not
         * exist. The broker will raise an error if the queue doesn't already exist
         * and passive is `true`. With passive `false` (the default), the queue gets
         * created automatically, if needed.
         * @param durable Indicates whether the exchange is durable - e.g., will it
         * survive a broker restart.
         * @param exclusive Indicates that only client can use the queue. Defaults to
         * true. An exclusive queue is deleted when the connection is closed.
         * @param auto_delete the queue will be deleted after at least one exchange
         * has been bound to it, then has been unbound
         * @returns The name of the queue created on the broker. Used mostly when the
         * broker is asked to create a unique queue by not providing a queue name.
         */
        virtual void queueDeclare(const std::string& queue_name, bool passive = false,
            bool durable = false, bool exclusive = true,
            bool auto_delete = true)
            = 0;

        /**
         * Binds a queue to an exchange
         *
         * Connects a queue to an exchange on the broker.
         * @param queue_name The name of the queue to bind.
         * @param exchange_name The name of the exchange to bind to.
         * @param routing_key Defines the routing key of the binding. Only messages
         * with matching routing key will be delivered to the queue from the exchange.
         * Defaults to `""` which means all messages will be delivered.
         */
        virtual void queueBind(
            const std::string& queue_name,
            const std::string& exchange_name,
            const std::string& routing_key = "")
            = 0;

        /**
         * Unbinds a queue from an exchange
         *
         * Disconnects a queue from an exchange.
         * @param queue_name The name of the queue to unbind.
         * @param exchange_name The name of the exchange to unbind.
         * @param routing_key This must match the routing_key of the binding.
         * @see BindQueue
         */
        virtual void queueUnbind(
            const std::string& queue_name,
            const std::string& exchange_name,
            const std::string& routing_key = "")
            = 0;

        virtual void basicPublish(
            const std::string& exchange,
            const std::string& routing_key,
            const Message2& message,
            bool mandatory = false,
            bool immediate = false)
            = 0;

        virtual void basicConsume(
            const std::string& queue_name,
            const std::string& consumer_tag = "",
            bool no_local = false,
            bool no_ack = true,
            bool exclusive = false)
            = 0;

        virtual void basicCancel(
            const std::string& consumer_tag)
            = 0;

        virtual void basicQos(
            uint32_t prefetch_size,
            uint16_t prefetch_count,
            bool global)
            = 0;

        /**
         * Acknowledges a Basic message
         *
         * Acknowledges a message delievered using \ref BasicGet or \ref BasicConsume.
         * @param message The message that is being ack'ed.
         */
        virtual void basicAck(uint64_t delivery_tag, bool multiple = false) = 0;

        virtual void basicNack(uint64_t delivery_tag, bool multiple = false, bool requeue = false) = 0;

        static Ptr createInstance(Connection2::Ptr);
    };

/**
 * @brief Class for specifying the RabbitMQ queue and exchange
 * parameters, i.e. "queue_declare", "queue_bind".
 */
struct Configuration {
	struct {
		std::string name = "";
		std::string type = "";
		bool passive = false;
		bool durable = false;
		bool auto_delete = false;
		bool internal = false;
		bool declare = false;
	} exchange;
	struct {
		std::string name = "";
		bool passive = false;
		bool durable = false;
		bool auto_delete = false;
		bool exclusive = false;
		bool declare = false;
		bool bind = false;
	} queue;
	std::string routing_key = "";
};

/**
 * @brief An AMQP Message, extends amqp_message_t struct
 */
class Message : protected amqp_message_t
{
public:
	typedef std::shared_ptr<Message> Ptr;

	inline Message()
	{
		properties._flags = 0;
	}

	inline Message(const std::string &body) : Message()
	{
		this->body = amqp_bytes_malloc_dup(amqp_cstring_bytes(body.c_str()));
	}

	inline Message(const amqp_message_t &message)
	{
		*this = message;
	}

	Message& operator=(const amqp_message_t & message)
	{
		this->body = amqp_bytes_malloc_dup(message.body);
		this->properties = message.properties;
		this->pool = message.pool;
		
		for (auto iter = propertyMap.begin(); iter != propertyMap.end(); iter++) {
			switch (iter->second.first & message.properties._flags) {
			case AMQP_BASIC_CONTENT_TYPE_FLAG:
				properties.content_type = amqp_bytes_malloc_dup(message.properties.content_type);
				break;
			case AMQP_BASIC_CONTENT_ENCODING_FLAG:
				properties.content_encoding = amqp_bytes_malloc_dup(message.properties.content_encoding);
				break;
			case AMQP_BASIC_DELIVERY_MODE_FLAG:
				properties.delivery_mode = message.properties.delivery_mode;
				break;
			case AMQP_BASIC_PRIORITY_FLAG:
				properties.priority = message.properties.priority;
				break;
			case AMQP_BASIC_CORRELATION_ID_FLAG:
				properties.correlation_id = amqp_bytes_malloc_dup(message.properties.correlation_id);
				break;
			case AMQP_BASIC_REPLY_TO_FLAG:
				properties.reply_to = amqp_bytes_malloc_dup(message.properties.reply_to);
				break;
			case AMQP_BASIC_EXPIRATION_FLAG:
				properties.expiration = amqp_bytes_malloc_dup(message.properties.expiration);
				break;
			case AMQP_BASIC_MESSAGE_ID_FLAG:
				properties.message_id = amqp_bytes_malloc_dup(message.properties.message_id);
				break;
			case AMQP_BASIC_TIMESTAMP_FLAG:
				properties.timestamp = message.properties.timestamp;
				break;
			case AMQP_BASIC_TYPE_FLAG:
				properties.type = amqp_bytes_malloc_dup(message.properties.type);
				break;
			case AMQP_BASIC_USER_ID_FLAG:
				properties.user_id = amqp_bytes_malloc_dup(message.properties.user_id);
				break;
			case AMQP_BASIC_APP_ID_FLAG:
				properties.app_id = amqp_bytes_malloc_dup(message.properties.app_id);
				break;
			case AMQP_BASIC_CLUSTER_ID_FLAG:
				properties.cluster_id = amqp_bytes_malloc_dup(message.properties.cluster_id);
				break;
			}
		}

		return *this;
	}

	inline virtual ~Message()
	{
		// In rabbitmq-c amqp_destroy_message() frees memory associated 
		// with a amqp_message_t allocated in amqp_read_message.
		// Instead, we manually delete fields depending on
		// how object was created, due to in many case amqp_destroy_envelope() implicitly
		// calls amqp_destroy_message().
		for (auto iter = propertyMap.begin(); iter != propertyMap.end(); iter++) {
			if (iter->second.first & properties._flags) {
				// Skip non amqp_bytes_t properties.
				if (!(iter->second.first & (AMQP_BASIC_DELIVERY_MODE_FLAG |
				                            AMQP_BASIC_PRIORITY_FLAG |
				                            AMQP_BASIC_TIMESTAMP_FLAG)))
				{
					amqp_bytes_t* ptr = reinterpret_cast<amqp_bytes_t*>(iter->second.second);
					if (ptr->bytes) {
						amqp_bytes_free(*ptr);
					}
				}
			}
		}
		// Skip this->pool as it is managed by rabbitmq-c internally.
		if (body.bytes) {
			amqp_bytes_free(body);
		}
	}

	inline void setProperty(const std::string & key, const char *value)
	{
		if (propertyMap.find(key) == propertyMap.end()) {
			return;
		}
		if (!(propertyMap[key].first & (AMQP_BASIC_DELIVERY_MODE_FLAG |
		                                AMQP_BASIC_PRIORITY_FLAG |
		                                AMQP_BASIC_TIMESTAMP_FLAG)))
		{
			*reinterpret_cast<amqp_bytes_t*>(propertyMap[key].second) = amqp_bytes_malloc_dup(amqp_cstring_bytes(value));
			properties._flags |= propertyMap[key].first;
		}
	}

	inline void setProperty(const std::string & key, uint8_t value)
	{
		if (propertyMap.find(key) == propertyMap.end()) {
			return;
		}
		if (propertyMap[key].first & (AMQP_BASIC_DELIVERY_MODE_FLAG | AMQP_BASIC_PRIORITY_FLAG)) {
			*reinterpret_cast<uint8_t*>(propertyMap[key].second) = value;
			properties._flags |= propertyMap[key].first;
		}
	}

	inline void setProperty(const std::string & key, uint64_t value)
	{
		if (propertyMap.find(key) == propertyMap.end()) {
			return;
		}
		if (propertyMap[key].first & AMQP_BASIC_TIMESTAMP_FLAG) {
			*reinterpret_cast<uint64_t*>(propertyMap[key].second) = value;
			properties._flags |= propertyMap[key].first;
		}
	}

	inline void setBody(const std::string & body)
	{
		this->body = amqp_bytes_malloc_dup(amqp_cstring_bytes(body.c_str()));
	}

	inline std::string getBody() const
	{
		return std::string((char *)body.bytes, (int)body.len);
	}

	friend class ChannelImpl;

protected:
	std::map<std::string, std::pair<amqp_flags_t, void*>> propertyMap {
		{"Content-Type", {AMQP_BASIC_CONTENT_TYPE_FLAG, &properties.content_type}},
		{"Content-Encoding", {AMQP_BASIC_CONTENT_ENCODING_FLAG, &properties.content_encoding}},
		{"Delivery-Mode", {AMQP_BASIC_DELIVERY_MODE_FLAG, &properties.delivery_mode}},
		{"Priority", {AMQP_BASIC_PRIORITY_FLAG, &properties.priority}},
		{"Correlation-Id", {AMQP_BASIC_CORRELATION_ID_FLAG, &properties.correlation_id}},
		{"Reply-To", {AMQP_BASIC_REPLY_TO_FLAG, &properties.reply_to}},
		{"Expiration", {AMQP_BASIC_EXPIRATION_FLAG, &properties.expiration}},
		{"Message-Id", {AMQP_BASIC_MESSAGE_ID_FLAG, &properties.message_id}},
		{"Timestamp", {AMQP_BASIC_TIMESTAMP_FLAG, &properties.timestamp}},
		{"Type", {AMQP_BASIC_TYPE_FLAG, &properties.type}},
		{"User-Id", {AMQP_BASIC_USER_ID_FLAG, &properties.user_id}},
		{"App-Id", {AMQP_BASIC_APP_ID_FLAG, &properties.app_id}},
		{"Cluster-Id", {AMQP_BASIC_CLUSTER_ID_FLAG, &properties.cluster_id}}
	};
};

using RpcReply = amqp_rpc_reply_t;

/**
 * @brief A "message envelope" object containing the message body 
 * and delivery metadata, extends amqp_envelope_t struct.
 */
class Envelope : public amqp_envelope_t
{
public:
	virtual ~Envelope()
	{
		// envelope a pointer to a amqp_envelope_t object. Caller
		// should call #amqp_destroy_envelope() when it is done using
		// the fields in the envelope object.
		amqp_destroy_envelope(this);
	}
};

/**
 * @brief Creates a new connection to an AMQP broker
 * using the supplied parameters.
 */
class Connection
{
	std::unique_ptr<class ConnectionImpl> m_impl;

public:
	/**
	 * @brief Establish an amqp connection by parameters used to connect to the RabbitMQ broker
	 * 
	 * @param[in]  host       The port (i.e. 127.0.0.1)
	 * @param[in]  port       The host (i.e. 5672)
	 * @param[in]  username   The username (i.e. guest)
	 * @param[in]  password   The password (i.e. guest)
	 * @param[in]  vhost      The vhost virtual host to connect to on the broker.
	 *                        The default on most brokers is "/" 
	 * @param[in]  frame_max  The maximum size of an AMQP frame on the wire to
	 *                        request of the broker for this connection
	 */
	Connection(
		const std::string &host = "127.0.0.1", int port = 5672,
		const std::string &username = "guest",
		const std::string &password = "guest",
		const std::string &vhost = "/", int frame_max = 131072);

	virtual ~Connection();

	void maybe_release_buffers();
	RpcReply consume_message(Envelope *envelope, const struct timeval *timeout, int flags);

	friend class ChannelImpl;
};

/**
 * @brief A single channel multiplexed in an AMQP connection.
 * Represents a logical AMQP channel multiplexed over a connection
 */
class Channel
{
	std::unique_ptr<class ChannelImpl> m_impl;

public:
	Channel(Connection *connection);
	virtual ~Channel();

	std::tuple<std::string,std::string> setup(const Configuration &configration);

	void basic_publish(const std::string &exchange, const std::string &routing_key, const Message &message, bool mandatory = false, bool immediate = false);

	void basic_consume(const std::string &queue_name, const std::string &consumer_tag = "", bool no_local = false, bool no_ack = true, bool exclusive = false);

	void basic_cancel(const std::string &consumer_tag);

	void basic_qos(uint32_t prefetch_size, uint16_t prefetch_count, bool global);

	void basic_ack(uint64_t delivery_tag, bool multiple = false);

	void basic_nack(uint64_t delivery_tag, bool multiple = false, bool requeue = false);
};

/**
 * @brief An AMQP message class intended for a "Request/Reply" pattern.
 * Use to build an RPC system: a client and a scalable RPC server. 
 */
class Request : public Message
{
public:
	using Message::Message;
};

/**
 * @brief An AMQP message class intended for a "Request/Reply" pattern.
 * Use to build an RPC system: a client and a scalable RPC server. 
 */
class Response : public Message
{
public:
	using Message::Message;

	typedef std::shared_ptr<Response> Ptr;

	bool ok() const {
		return std::string((char*)this->properties.type.bytes,
			this->properties.type.len) != "error";
	}
};

} // end namespace amqp

class MessageBroker
{
public:
	typedef std::shared_ptr<MessageBroker> Ptr;

	using Configuration = amqp::Configuration;
	using Message = amqp::Message;
	using Request = amqp::Request;
	using Response = amqp::Response;

	/**
	 * @brief Parse a connection URL and establish an amqp connection.
	 * An amqp connection url takes the form:
	 * amqp://[$USERNAME[:$PASSWORD]\@]$HOST[:$PORT]/[$VHOST]
	 * Examples:
	 *    amqp://guest:guest\@localhost:5672//
	 *	  amqp://guest:guest\@localhost/myvhost
	 * Any missing parts of the URL will be set to the defaults specified in
	 * amqp_default_connection_info.
	 */
	MessageBroker(const std::string &url, int frame_max = 131072);

	/**
	 * @brief Establish an amqp connection by parameters used to connect to the RabbitMQ broker
	 * 
	 * @param[in]  host       The port (i.e. 127.0.0.1)
	 * @param[in]  port       The host (i.e. 5672)
	 * @param[in]  username   The username (i.e. guest)
	 * @param[in]  password   The password (i.e. guest)
	 * @param[in]  vhost      The vhost virtual host to connect to on the broker.
	 *                        The default on most brokers is "/" 
	 * @param[in]  frame_max  The maximum size of an AMQP frame on the wire to
	 *                        request of the broker for this connection
	 */
	MessageBroker(
		const std::string &host, int port,
		const std::string &username,
		const std::string &password,
		const std::string &vhost, int frame_max = 131072);

	virtual ~MessageBroker();

	/// Basic messaging pattern for publish events.
	void publish(const Configuration &configuration, const std::string &messagebody);

	/// RPC messaging pattern for publish events.
	Response::Ptr publish(const Configuration &configuration, const std::string &messagebody, struct timeval *timeout);

	/// Basic messaging pattern for event subscription.
	void subscribe(const Configuration &configuration, std::function<void (const Message&)> callback);

	/// RPC messaging pattern for event subscription.
	void subscribe(const Configuration &configuration, std::function<bool (const Request&, Response&)> callback);

	/// Close all subscription and join threads.
	void close();

protected:
	std::string m_host;
	int m_port;
	std::string m_username;
	std::string m_password;
	std::string m_vhost;
	int m_frame_max;
	std::vector<std::thread> m_threads;
	std::atomic<bool> m_close{false};
};

} // end namespace soft

#endif // MESSAGE_BROKER_H
