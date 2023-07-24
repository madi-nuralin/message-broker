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
		std::string serialize() const;
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
		Response(const Request &request, bool ok = true) : Message() {
			m_type = ok ? "response" : "error";
			m_reqid = request.reqid();
		}

		Response(const Request &request, const std::string &body, bool ok = true) : Message(body) {
			m_type = ok ? "response" : "error";
			m_reqid = request.reqid();
		}

		~Response() {}

		bool ok() const { return m_type != "error"; }

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
              bool auto_delete = false)
		: name(queue_name),
		  passive(passive),
		  durable(durable),
		  exclusive(exclusive),
		  auto_delete(auto_delete) {}

		std::string name;
		bool passive, durable, auto_delete, exclusive;
	};

	struct BasicMessage
	{
		enum delivery_mode_t {
			dm_notset = 0,
			dm_nonpersistent = 1,
			dm_persistent = 2
		};

		BasicMessage(const std::string &body) : body(body) {}

		std::string body;
		struct {
			std::string content_type;
			std::string content_encoding;
			delivery_mode_t delivery_mode;
			uint8_t priority;
			std::string correlation_id;
			std::string reply_to;
			std::string expiration;
			std::string message_id;
			uint64_t timestamp;
			std::string type;
			std::string user_id;
			std::string app_id;
			std::string cluster_id;
			//Table header_table;
		} properties;
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

		void declareExchange(const Exchange &exchange);
		
		void declareQueue(const Queue &queue);
		
		void bindQueue(const std::string &queuename,
                       const std::string &exchange,
                       const std::string &routingkey);

		void basicPublish(const std::string &exchange,
                          const std::string &routingkey,
                          const BasicMessage &message,
                          bool mandatory = false,
                          bool immediate = false);
	
	};
	
	void publish(const Exchange &exchange, const Queue &queue, const std::string &routingkey, const std::string &message);
	//void publish(const std::string &exchange, const std::string &routingkey, const std::string &messagebody);
	//void publish(const std::string& exchange, const std::string& routingkey, const std::string& messagebody, void (*callback)(const Response& response));
	//void subscribe(const std::string& bindingkey, void (*callback)(const Envelope& envelope));
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
