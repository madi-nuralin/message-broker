#ifndef MESSAGE_BROKER_HPP
#define MESSAGE_BROKER_HPP

#include <string>
#include <memory>
#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <json-glib/json-glib.h>
#include <json-glib/json-gobject.h>

class MessageBroker
{
public:
	/**
	 * @brief      Constructs a new instance.
	 *
	 * @param[in]  host       The host
	 * @param[in]  port       The port
	 * @param[in]  username   The username
	 * @param[in]  password   The password
	 * @param[in]  vhost      The vhost
	 * @param[in]  frame_max  The frame maximum
	 */
	MessageBroker(const std::string &host = "127.0.0.1", int port = 5672,
                  const std::string &username = "guest",
                  const std::string &password = "guest",
                  const std::string &vhost = "/", int frame_max = 131072);

	/**
	 * @brief      Destroys the object.
	 */
	~MessageBroker();

	class Message
	{
	public:
		Message();
		Message(const std::string& body);

		~Message();

		std::string reqid() const { return m_reqid; }
		bool setBody(const JsonNode *json_node, std::string *error = nullptr);
		bool setBody(const std::string &body, std::string *error = nullptr);
		virtual std::string serialize() const;
		std::string serializeBody() const;

	protected:
		std::string m_reqid, m_type;
		JsonNode *m_body;
	};

	class Request : public Message
	{
	public:
		Request() : Message() {
			m_type = "request";
		}

		Request(const std::string &body) : Message(body) {
			m_type = "request";
		}

		~Request() {}
	};

	class Response : public Message
	{
	public:
		Response(const Request &request) : Message() {
			m_reqid = request.reqid();
			m_type = "response";
		}

		Response(const Request &request, const std::string &body) : Message(body) {
			m_reqid = request.reqid();
			m_type = "response";
		}

		~Response() {}

		std::string serialize() const override;
		void setReason(const std::string &reason);
		std::string reason() const { return m_reason; }
		bool ok() const { return m_type != "error"; }

	protected:
		std::string m_reason;

	private:
		Response();
	};

	struct Exchange
	{
		Exchange(const std::string &exchange_name,
                 const std::string &exchange_type = "direct",
                 bool passive = false,
                 bool durable = false,
                 bool auto_delete = false,
                 bool internal = false)
		: name(exchange_name),
		  type(exchange_type),
		  passive(passive),
		  durable(durable),
		  auto_delete(auto_delete),
		  internal(internal) {}

		std::string name, type;
		bool passive, durable, auto_delete, internal;
	};

	struct Queue
	{
              Queue(const std::string &queue_name,
              bool passive = false,
              bool durable = false,
              bool exclusive = false,
              bool auto_delete = true)
		: name(queue_name),
		  passive(passive),
		  durable(durable),
		  exclusive(exclusive),
		  auto_delete(auto_delete) {}

		std::string name;
		bool passive, durable, auto_delete, exclusive;
	};

	struct BasicMessage : public amqp_message_t
	{
		typedef std::shared_ptr<BasicMessage> Ptr;

		BasicMessage(const std::string &body) {
			this->body = amqp_cstring_bytes(body.c_str());
		}

		struct PropertyDescriptor {
			uint16_t flag;
			void *ptr;
		};

		std::map<std::string, PropertyDescriptor> m {
			{"Content-Type", PropertyDescriptor{AMQP_BASIC_CONTENT_TYPE_FLAG, &content_type}},
			{"Content-Encoding", PropertyDescriptor{AMQP_BASIC_CONTENT_ENCODING_FLAG, &content_encoding}},
			{"Delivery-Mode", PropertyDescriptor{AMQP_BASIC_DELIVERY_MODE_FLAG, &delivery_mode}},
			{"Priority", PropertyDescriptor{AMQP_BASIC_PRIORITY_FLAG, &priority}},
			{"Correlation-Id", PropertyDescriptor{AMQP_BASIC_CORRELATION_ID_FLAG, &correlation_id}},
			{"Reply-To", PropertyDescriptor{AMQP_BASIC_RELY_TO_FLAG, &reply_to}},
			{"Expiration", PropertyDescriptor{AMQP_BASIC_EXIRATION_FLAG, &expiration}},
			{"Message-Id", PropertyDescriptor{AMQP_BASIC_MESSAGE_ID_FLAG, &message_id}},
			{"Timestamp", PropertyDescriptor{AMQP_BASIC_TIMESTAMP_FLAG, &timestamp}},
			{"Type", PropertyDescriptor{AMQP_BASIC_TYPE_FLAG, &type}},
			{"User-Id", PropertyDescriptor{AMQP_BASIC_USER_ID_FLAG, &user_id}},
			{"App-Id", PropertyDescriptor{AMQP_BASIC_APP_ID_FLAG, &app_id}},
			{"Cluster-Id", PropertyDescriptor{AMQP_BASIC_CLUSTER_ID_FLAG, &cluster_id}}
		};

		void setProperty(const std::string & property, const char *value) {
			*reinterpret_cast<amqp_bytes_t*>(m[property].ptr) = amqp_cstring_bytes(value);
			this->_flags |= m[property].flag;
		}

		void setProperty(const std::string & property, uint8_t value) {
			*reinterpret_cast<uint8_t*>(m[property].ptr) = value;
			this->_flags |= m[property].flag;
		}

		void setProperty(const std::string & property, uint64_t value) {
			*reinterpret_cast<uint64_t*>(m[property].ptr) = value;
			this->_flags |= m[property].flag;
		}
	};

	struct Envelope : public amqp_envelope_t
	{
		typedef std::shared_ptr<Envelope> Ptr;
	};

	class Connection
	{
	public:
		typedef std::shared_ptr<Connection> Ptr;

		/**
		 * @brief      Constructs a new instance.
		 *
		 * @param[in]  host       The host
		 * @param[in]  port       The port
		 * @param[in]  username   The username
		 * @param[in]  password   The password
		 * @param[in]  vhost      The vhost
		 * @param[in]  frame_max  The frame maximum
		 */
		Connection(const std::string &host = "127.0.0.1", int port = 5672,
                   const std::string &username = "guest",
                   const std::string &password = "guest",
                   const std::string &vhost = "/", int frame_max = 131072);

		/**
		 * @brief      Destroys the object.
		 */
		~Connection();

		amqp_socket_t *socket = NULL;
		amqp_connection_state_t conn;

		void declareExchange(Exchange &exchange);
		
		void declareQueue(Queue &queue);
		
		void bindQueue(const std::string &queuename,
                       const std::string &exchange,
                       const std::string &routingkey);

		void basicPublish(const std::string &exchange,
                          const std::string &routingkey,
                          const BasicMessage &message,
                          bool mandatory = false,
                          bool immediate = false);

		void basicConsume(const std::string &queuename,
                          const std::string &consumer_tag = "",
                          bool no_local = true, bool no_ack = true,
                          bool exclusive = true,
                          uint16_t message_prefetch_count = 1);
	};
	
	void publish(Exchange &exchange, Queue &queue, const std::string &routingkey, const std::string &message);
	void publish(const std::string &exchange, const std::string &routingkey, const std::string &messagebody);
	//void publish(const std::string& exchange, const std::string& routingkey, const std::string& messagebody, void (*callback)(const Response& response));
	void subscribe(const std::string &exchange, const std::string &bindingkey, void (*callback)(const Message& message));
	//void subscribe(const std::string& bindingkey, bool (*callback)(const Request& request, Response& response));

private:
	std::string m_host;
	std::string m_username;
	std::string m_password;
	std::string m_vhost;
	int m_port;
	int m_frame_max;

	Connection::Ptr m_connection;
};

#endif //MESSAGE_BROKER_HPP
