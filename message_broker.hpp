#ifndef  MESSAGE_BROKER_H
#define  MESSAGE_BROKER_H

#include <map>
#include <string>
#include <utility>
#include <functional>
#include <memory>

#include <rabbitmq-c/amqp.h>
#include <rabbitmq-c/tcp_socket.h>

namespace gammasoft {

namespace amqp {

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
class Message : public amqp_message_t
{
public:
	typedef std::shared_ptr<Message> Ptr;

	Message() {
		properties._flags = 0;
	}

	Message(const std::string &body) : Message() {
		this->body = amqp_bytes_malloc_dup(amqp_cstring_bytes(body.c_str()));
	}

	Message(const amqp_message_t &message) {
		body = amqp_bytes_malloc_dup(message.body);
		properties = message.properties;
		pool = message.pool;
		from_copy = false;

		for (auto iter = propertyMap.begin(); iter != propertyMap.end(); iter++) {
			switch (iter->second.flag & message.properties._flags) {
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
	}

	Message& operator=(const amqp_message_t & message) {
		body = message.body;
		properties = message.properties;
		pool = message.pool;
		return *this;
	}

	~Message() {
		// In rabbitmq-c amqp_destroy_message() frees memory associated 
		// with a amqp_message_t allocated in amqp_read_message.
		// Instead (if message was not "copied"), we manually delete fields depending on
		// how object was created, due to in many case ~Envelope() implicitly
		// calls amqp_destroy_message().
		if (!from_copy) {
			for (auto iter = propertyMap.begin(); iter != propertyMap.end(); iter++) {
				// Skip non amqp_bytes_t types.
				if (!(iter->second.flag & (
					AMQP_BASIC_DELIVERY_MODE_FLAG |
					AMQP_BASIC_PRIORITY_FLAG |
					AMQP_BASIC_TIMESTAMP_FLAG))) {
					amqp_bytes_t* ptr = reinterpret_cast<amqp_bytes_t*>(iter->second.ptr);
					if (ptr->bytes) {
						amqp_bytes_free(*ptr);
					}
				}
			}
			// Skip this->pool as it is managed by rabbitmq-c internally.
			if (body.bytes) {
				amqp_bytes_free(body);
			}
		}
	}

	void setProperty(const std::string & key, const char *value) {
		*reinterpret_cast<amqp_bytes_t*>(propertyMap[key].ptr) = amqp_cstring_bytes(value);
		properties._flags |= propertyMap[key].flag;
	}
	void setProperty(const std::string & key, uint8_t value) {
		*reinterpret_cast<uint8_t*>(propertyMap[key].ptr) = value;
		properties._flags |= propertyMap[key].flag;
	}
	void setProperty(const std::string & key, uint64_t value) {
		*reinterpret_cast<uint64_t*>(propertyMap[key].ptr) = value;
		properties._flags |= propertyMap[key].flag;
	}

	void setBody(const std::string & body) {
		this->body = amqp_bytes_malloc_dup(amqp_cstring_bytes(body.c_str()));
	}
	std::string getBody() const {
		return std::string((char *)body.bytes, (int)body.len);
	}

protected:
	bool from_copy = true;

	struct PropertyDescriptor {
		amqp_flags_t flag;
		void *ptr;
	};

	std::map<std::string, PropertyDescriptor> propertyMap {
		{"Content-Type", PropertyDescriptor{AMQP_BASIC_CONTENT_TYPE_FLAG, &properties.content_type}},
		{"Content-Encoding", PropertyDescriptor{AMQP_BASIC_CONTENT_ENCODING_FLAG, &properties.content_encoding}},
		{"Delivery-Mode", PropertyDescriptor{AMQP_BASIC_DELIVERY_MODE_FLAG, &properties.delivery_mode}},
		{"Priority", PropertyDescriptor{AMQP_BASIC_PRIORITY_FLAG, &properties.priority}},
		{"Correlation-Id", PropertyDescriptor{AMQP_BASIC_CORRELATION_ID_FLAG, &properties.correlation_id}},
		{"Reply-To", PropertyDescriptor{AMQP_BASIC_REPLY_TO_FLAG, &properties.reply_to}},
		{"Expiration", PropertyDescriptor{AMQP_BASIC_EXPIRATION_FLAG, &properties.expiration}},
		{"Message-Id", PropertyDescriptor{AMQP_BASIC_MESSAGE_ID_FLAG, &properties.message_id}},
		{"Timestamp", PropertyDescriptor{AMQP_BASIC_TIMESTAMP_FLAG, &properties.timestamp}},
		{"Type", PropertyDescriptor{AMQP_BASIC_TYPE_FLAG, &properties.type}},
		{"User-Id", PropertyDescriptor{AMQP_BASIC_USER_ID_FLAG, &properties.user_id}},
		{"App-Id", PropertyDescriptor{AMQP_BASIC_APP_ID_FLAG, &properties.app_id}},
		{"Cluster-Id", PropertyDescriptor{AMQP_BASIC_CLUSTER_ID_FLAG, &properties.cluster_id}}
	};
};

/**
 * @brief A "message envelope" object containing the message body and delivery metadata.
 */
using Envelope = amqp_envelope_t;

/**
 * @brief Creates a new connection to an AMQP broker
 * using the supplied parameters.
 */
class Connection
{
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

	~Connection();

	amqp_connection_state_t state;
};

/**
 * @brief A single channel multiplexed in an AMQP connection.
 * Represents a logical AMQP channel multiplexed over a connection
 */
class Channel
{
public:
	Channel(Connection *connection);
	~Channel();

	std::tuple<std::string,std::string> setup(const Configuration &configration);
	
	void publish(const std::string &exchange, const std::string &routing_key, const Message &message, bool mandatory = false, bool immediate = false);

	void consume(const std::string &queue_name, struct timeval *timeout, std::function<void(const Envelope &)>, const std::string &consumer_tag = "", bool no_local = false, bool no_ack = true, bool exclusive = false);

	void cancel(const std::string &consumer_tag);

	void qos(uint32_t prefetch_size, uint16_t prefetch_count, bool global);

	void acknowledge(uint64_t delivery_tag, bool multiple = false);

	void reject(uint64_t delivery_tag, bool multiple = false, bool requeue = false);

	void stop_consuming() {
		consumer_loop = false;
	}

private:
	amqp_channel_t id;
	Connection *connection;
	bool consumer_loop;
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

	using Connection = amqp::Connection;
	using Channel = amqp::Channel;
	using Configuration = amqp::Configuration;
	using Envelope = amqp::Envelope;
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

private:
	std::string m_host;
	int m_port;
	std::string m_username;
	std::string m_password;
	std::string m_vhost;
	int m_frame_max;
};

} // end namespace gammasoft

#endif // MESSAGE_BROKER_H
