#ifndef MESSAGE_BROKER_H
#define MESSAGE_BROKER_H

#include <atomic>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace soft {

namespace amqp {

struct AmqpProperties
{
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

class AmqpMessage
{
public:
  using Ptr = std::shared_ptr<AmqpMessage>;
  using WPtr = std::weak_ptr<AmqpMessage>;

  AmqpMessage();
  virtual ~AmqpMessage();

  virtual std::string& body() noexcept = 0;
  virtual void body(const std::string&) noexcept = 0;

  virtual AmqpProperties& properties() noexcept = 0;
  virtual void properties(const AmqpProperties&) noexcept = 0;

  static Ptr createInstance();
};

class AmqpEnvelope
{
public:
  using Ptr = std::shared_ptr<AmqpEnvelope>;
  using WPtr = std::weak_ptr<AmqpEnvelope>;

  AmqpEnvelope();
  virtual ~AmqpEnvelope();

  virtual AmqpMessage::Ptr message() const noexcept = 0;
  virtual void message(AmqpMessage::Ptr) noexcept = 0;

  virtual std::string consumerTag() const noexcept = 0;
  virtual consumerTag(const std::string&) noexcept = 0;

  virtual std::uint64_t deliveryTag() const noexcept = 0;

  virtual std::string exchange() const noexcept = 0;

  virtual bool redelivered() const noexcept = 0;

  virtual std::string routingKey() const noexcept = 0;

  static Ptr createInstance();
};

class AmqpConnection
{
public:
  using Ptr = std::shared_ptr<AmqpConnection>;
  using WPtr = std::weak_ptr<AmqpConnection>;

  AmqpConnection();
  virtual ~AmqpConnection();

  virtual void open(const std::string& host, int port) = 0;
  virtual void login(const std::string& vhost,
                     const std::string& username,
                     const std::string& password,
                     int frame_max) const = 0;

  static Ptr createInstance();
};

class AmqpChannel
{
  ///< `"direct"` string constant
  static const char* EXCHANGE_TYPE_DIRECT;

  ///< `"fanout"` string constant
  static const char* EXCHANGE_TYPE_FANOUT;

  ///< `"topic"` string constant
  static const char* EXCHANGE_TYPE_TOPIC;

public:
  using Ptr = std::shared_ptr<AmqpChannel>;
  using WPtr = std::weak_ptr<AmqpChannel>;

  AmqpChannel();
  virtual ~AmqpChannel();

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
    const std::string& exchange_type = AmqpChannel::EXCHANGE_TYPE_DIRECT,
    bool passive = false,
    bool durable = false,
    bool auto_delete = false,
    bool internal = false) = 0;

  /**
   * Binds one exchange to another exchange using a given key
   * @param destination the name of the exchange to route messages to
   * @param source the name of the exchange to route messages from
   * @param routing_key the routing key to use when binding
   */
  virtual void exchangeBind(const std::string& destination,
                            const std::string& source,
                            const std::string& routing_key) = 0;

  /**
   * Unbind an existing exchange-exchange binding
   * @see BindExchange
   * @param destination the name of the exchange to route messages to
   * @param source the name of the exchange to route messages from
   * @param routing_key the routing key to use when binding
   */
  virtual void exchangeUnbind(const std::string& destination,
                              const std::string& source,
                              const std::string& routing_key) = 0;

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
  virtual std::string queueDeclare(const std::string& queue_name,
                                   bool passive = false,
                                   bool durable = false,
                                   bool exclusive = true,
                                   bool auto_delete = true) = 0;

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
  virtual void queueBind(const std::string& queue_name,
                         const std::string& exchange_name,
                         const std::string& routing_key = "") = 0;

  /**
   * Unbinds a queue from an exchange
   *
   * Disconnects a queue from an exchange.
   * @param queue_name The name of the queue to unbind.
   * @param exchange_name The name of the exchange to unbind.
   * @param routing_key This must match the routing_key of the binding.
   * @see BindQueue
   */
  virtual void queueUnbind(const std::string& queue_name,
                           const std::string& exchange_name,
                           const std::string& routing_key = "") = 0;

  virtual void basicPublish(const std::string& exchange,
                            const std::string& routing_key,
                            const AmqpMessage::Ptr message,
                            bool mandatory = false,
                            bool immediate = false) = 0;

  virtual void basicConsume(const std::string& queue_name,
                            const std::string& consumer_tag = "",
                            bool no_local = false,
                            bool no_ack = true,
                            bool exclusive = false) = 0;

  virtual void basicCancel(const std::string& consumer_tag) = 0;

  virtual void basicQos(uint32_t prefetch_size,
                        uint16_t prefetch_count,
                        bool global) = 0;

  /**
   * Acknowledges a Basic message
   *
   * Acknowledges a message delievered using \ref BasicGet or \ref BasicConsume.
   * @param message The message that is being ack'ed.
   */
  virtual void basicAck(uint64_t delivery_tag, bool multiple = false) = 0;

  virtual void basicNack(uint64_t delivery_tag,
                         bool multiple = false,
                         bool requeue = false) = 0;
  /**
   * Consumes a single message
   *
   * Waits for a single Basic message to be Delivered.
   *
   * This function only works after `BasicConsume` has successfully been called.
   *
   * @param consumer_tag Consumer ID (returned from \ref BasicConsume).
   * @returns The next message on the queue
   */
  virtual AmqpEnvelope::Ptr basicConsumeMessage(
    const std::string& consumer_tag) = 0;

  static Ptr createInstance(AmqpConnection::Ptr);
};

} // end namespace amqp

class MessageBroker
{
public:
  using Ptr = std::shared_ptr<MessageBroker>;
  using WPtr = std::weak_ptr<MessageBroker>;

  using Properties = amqp::AmqpProperties;
  using Message = amqp::AmqpMessage;

  struct Configuration
  {
    Properties properties;
    struct
    {
      std::string name = "";
      std::string type = "";
      bool passive = false;
      bool durable = false;
      bool auto_delete = false;
      bool internal = false;
      bool declare = false;
    } exchange;
    struct
    {
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
   * @brief      An AMQP message class intended for a "Request/Reply" pattern.
   *             Use to build an RPC system: a client and a scalable RPC server.
   */
  class Request : public Message
  {
  public:
    using Ptr = std::shared_ptr<Request>;
    using WPtr = std::weak_ptr<Request>;
  };

  /**
   * @brief      An AMQP message class intended for a "Request/Reply" pattern.
   *             Use to build an RPC system: a client and a scalable RPC server.
   */
  class Response : public Message
  {
  public:
    using Ptr = std::shared_ptr<Response>;
    using WPtr = std::weak_ptr<Response>;

    virtual bool ok() const = 0;
  };

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
  MessageBroker(const std::string& url, int frame_max = 131072);

  /**
   * @brief Establish an amqp connection by parameters used to connect to the
   * RabbitMQ broker
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
  MessageBroker(const std::string& host,
                int port,
                const std::string& username,
                const std::string& password,
                const std::string& vhost,
                int frame_max = 131072);

  virtual ~MessageBroker();

  /// Basic messaging pattern for publish events.
  ///
  /// @param[in]  configuration  The configuration
  /// @param[in]  messagebody    The messagebody
  ///
  void publish(const Configuration& configuration,
               const std::string& messagebody);

  /// RPC messaging pattern for publish events.
  ///
  /// @param[in]  configuration  The configuration
  /// @param[in]  messagebody    The messagebody
  /// @param      timeout        The timeout
  ///
  /// @return     { description_of_the_return_value }
  ///
  Response::Ptr publish(const Configuration& configuration,
                        const std::string& messagebody,
                        struct timeval* timeout);

  /// Basic messaging pattern for event subscription.
  ///
  /// @param[in]  configuration  The configuration
  /// @param[in]  callback       The callback
  ///
  void subscribe(const Configuration& configuration,
                 std::function<void(const Message::Ptr)> callback);

  /// RPC messaging pattern for event subscription.
  ///
  /// @param[in]  configuration  The configuration
  /// @param[in]  callback       The callback
  ///
  void subscribe(
    const Configuration& configuration,
    std::function<bool(const Request::Ptr, Response::Ptr)> callback);

  /// Close all subscription and join threads.
  ///
  void close();

protected:
  std::tuple<std::string, std::string> setup(amqp::AmqpChannel::Ptr,
                                             const Configuration&);

  std::string m_host;
  int m_port;
  std::string m_username;
  std::string m_password;
  std::string m_vhost;
  int m_frame_max;
  std::vector<std::thread> m_threads;
  std::atomic<bool> m_close{ false };
};

} // end namespace soft

#endif // MESSAGE_BROKER_H
