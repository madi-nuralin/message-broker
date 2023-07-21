#ifndef MESSAGE_BROKER_HPP
#define MESSAGE_BROKER_HPP

#include <string>
#include <memory>
#include <json-glib/json-glib.h>
#include <json-glib/json-gobject.h>

class MessageBroker
{
public:
	MessageBroker() = delete;

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

	/**
	 * @brief      { function_description }
	 *
	 * @param[in]  exchange     The exchange
	 * @param[in]  routingkey   The routingkey
	 * @param[in]  messagebody  The messagebody
	 */
	void publish(const std::string &exchange, const std::string &routingkey, const std::string &messagebody);

	/**
	 * @brief      { function_description }
	 *
	 * @param[in]  exchange     The exchange
	 * @param[in]  routingkey   The routingkey
	 * @param[in]  messagebody  The messagebody
	 * @param[in]  callback     The callback
	 */
	//void publish(const std::string& exchange, const std::string& routingkey, const std::string& messagebody, void (*callback)(const Response& response));

	/**
	 * @brief      { function_description }
	 *
	 * @param[in]  bindingkey  The bindingkey
	 * @param[in]  callback    The callback
	 */
	void subscribe(const std::string& bindingkey, void (*callback)(const Envelope& envelope));

	/**
	 * @brief      { function_description }
	 *
	 * @param[in]  bindingkey  The bindingkey
	 * @param[in]  callback    The callback
	 */
	//void subscribe(const std::string& bindingkey, bool (*callback)(const Request& request, Response& response));

	class Envelope
	{
	public:
		/**
		 * @brief      Constructs a new instance.
		 */
		Envelope();

		/**
		 * @brief      Constructs a new instance.
		 *
		 * @param[in]  body  The body
		 */
		Envelope(const std::string& body);

		/**
		 * @brief      Destroys the object.
		 */
		~Envelope();

		/**
		 * @brief      Sets the body.
		 *
		 * @param[in]  node  The node
		 *
		 * @return     { description_of_the_return_value }
		 */
		bool setBody(const JsonNode *node);

		/**
		 * @brief      Sets the body.
		 *
		 * @param[in]  str   The new value
		 *
		 * @return     { description_of_the_return_value }
		 */
		bool setBody(const std::string& str);

		/**
		 * @brief      { function_description }
		 *
		 * @return     { description_of_the_return_value }
		 */
		std::string serialize() const;

		/**
		 * @brief      { function_description }
		 *
		 * @return     { description_of_the_return_value }
		 */
		std::string serializeBody() const;

	protected:
		std::string m_reqid, m_type;
		g_autoptr(JsonNode) m_body;
	};

	class Request : public Envelope
	{
	public:
		/**
		 * @brief      Constructs a new instance.
		 */
		Request() = default;

		/**
		 * @brief      Constructs a new instance.
		 *
		 * @param[in]  body  The body
		 */
		Request(const std::string& body);

		/**
		 * @brief      Destroys the object.
		 */
		~Request();
		
	};

	class Response : public Envelope
	{
	public:
		typedef std::shared_ptr<Response> Ptr;

		/**
		 * @brief      Constructs a new instance.
		 */
		Response() = delete;
		
		/**
		 * @brief      Constructs a new instance.
		 *
		 * @param[in]  request  The request
		 */
		Response(const Request& request) = default;

		/**
		 * @brief      Constructs a new instance.
		 *
		 * @param[in]  request  The request
		 * @param[in]  body     The body
		 */
		Response(const Request& request, const std::string& body);

		/**
		 * @brief      Destroys the object.
		 */
		~Response();
		
		/**
		 * @brief      { function_description }
		 *
		 * @return     { description_of_the_return_value }
		 */
		bool ok() const {
			return m_type != "error";
		}
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
	};

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
