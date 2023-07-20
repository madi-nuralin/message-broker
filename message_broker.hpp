#ifndef MESSAGE_BROKER_HPP
#define MESSAGE_BROKER_HPP

#include <string>
#include <memory>

class MessageBroker
{
public:
	MessageBroker(const std::string& hostname, const int port);
	~MessageBroker();
	
	bool publish(const std::string& exchange, const std::string& routingkey, const std::string& messagebody);
	Response::Ptr publish(const std::string& exchange, const std::string& routingkey, const std::string& messagebody, const int timeout);
	bool subscribe(const std::string& exchange, void (*callback)(const Message& message));
	bool subscribe(const std::string& exchange, bool (*callback)(const Request& request, Response& response));

	class Message
	{
	public:
		Message();
		Message(const std::string& body);
		~Message();

		bool setBody(const JsonNode *node);
		bool setBody(const std::string& str);
		std::string serialize() const;
		std::string serializeBody() const;

	protected:
		std::string m_reqId, m_type;
		g_autoptr(JsonNode) m_body;
	};

	class Request : public Message
	{
	public:
		Request();
		~Request();
		
	};

	class Response : public Message
	{
	public:
		typedef std::shared_ptr<Response> Ptr;

		Response();
		~Response();
		
		bool ok() const;		
	};

	class Connection
	{
	public:
		Connection();
		~Connection();
		
	};

	class Queue
	{
	public:
		Queue();
		~Queue();
		
	};

private:

};

#endif //MESSAGE_BROKER_HPP
