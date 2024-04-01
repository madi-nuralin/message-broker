#include "message_broker.hpp"
#include <assert.h>
#include <atomic>
#include <chrono>
#include <rabbitmq-c/amqp.h>
#include <rabbitmq-c/tcp_socket.h>
#include <stdexcept>
#include <stdlib.h>
#include <string.h>
#include <thread>
#include <variant>
#include <vector>

#include "utils.h"

namespace gs {

namespace amqp {

static inline std::string
amqp_bytes_string(const amqp_bytes_t& x)
{
  return std::string((char*)x.bytes, x.len);
}

inline static amqp_bytes_t
string_amqp_bytes(const std::string& x)
{
  return amqp_cstring_bytes(x.c_str());
}

static inline AmqpProperties
convert_to_amqp_properties(const amqp_basic_properties_t& props)
{
  AmqpProperties properties;
  if (props._flags & AMQP_BASIC_CONTENT_TYPE_FLAG)
    properties.content_type = amqp_bytes_string(props.content_type);
  if (props._flags & AMQP_BASIC_CONTENT_ENCODING_FLAG)
    properties.content_encoding = amqp_bytes_string(props.content_encoding);
  if (props._flags & AMQP_BASIC_DELIVERY_MODE_FLAG)
    properties.delivery_mode = props.delivery_mode;
  if (props._flags & AMQP_BASIC_PRIORITY_FLAG)
    properties.priority = props.priority;
  if (props._flags & AMQP_BASIC_CORRELATION_ID_FLAG)
    properties.correlation_id = amqp_bytes_string(props.correlation_id);
  if (props._flags & AMQP_BASIC_REPLY_TO_FLAG)
    properties.reply_to = amqp_bytes_string(props.reply_to);
  if (props._flags & AMQP_BASIC_EXPIRATION_FLAG)
    properties.expiration = amqp_bytes_string(props.expiration);
  if (props._flags & AMQP_BASIC_MESSAGE_ID_FLAG)
    properties.message_id = amqp_bytes_string(props.message_id);
  if (props._flags & AMQP_BASIC_TIMESTAMP_FLAG)
    properties.timestamp = props.timestamp;
  if (props._flags & AMQP_BASIC_USER_ID_FLAG)
    properties.user_id = amqp_bytes_string(props.user_id);
  if (props._flags & AMQP_BASIC_APP_ID_FLAG)
    properties.app_id = amqp_bytes_string(props.app_id);
  if (props._flags & AMQP_BASIC_CLUSTER_ID_FLAG)
    properties.cluster_id = amqp_bytes_string(props.cluster_id);
  return properties;
}

inline static amqp_basic_properties_t
convert_to_amqp_basic_properties(const AmqpProperties& properties)
{
  amqp_basic_properties_t props;
  props._flags = 0;
  if (properties.content_type.has_value()) {
    props._flags |= AMQP_BASIC_CONTENT_TYPE_FLAG;
    props.content_type =
      amqp_cstring_bytes(properties.content_type.value().c_str());
  }
  if (properties.content_encoding.has_value()) {
    props._flags |= AMQP_BASIC_CONTENT_ENCODING_FLAG;
    props.content_encoding =
      amqp_cstring_bytes(properties.content_encoding.value().c_str());
  }
  if (properties.delivery_mode.has_value()) {
    props._flags |= AMQP_BASIC_DELIVERY_MODE_FLAG;
    props.delivery_mode = properties.delivery_mode.value();
  }
  if (properties.priority.has_value()) {
    props._flags |= AMQP_BASIC_PRIORITY_FLAG;
    props.priority = properties.priority.value();
  }
  if (properties.correlation_id.has_value()) {
    props._flags |= AMQP_BASIC_CORRELATION_ID_FLAG;
    props.correlation_id =
      amqp_cstring_bytes(properties.correlation_id.value().c_str());
  }
  if (properties.reply_to.has_value()) {
    props._flags |= AMQP_BASIC_REPLY_TO_FLAG;
    props.reply_to = amqp_cstring_bytes(properties.reply_to.value().c_str());
  }
  if (properties.expiration.has_value()) {
    props._flags |= AMQP_BASIC_EXPIRATION_FLAG;
    props.expiration =
      amqp_cstring_bytes(properties.expiration.value().c_str());
  }
  if (properties.message_id.has_value()) {
    props._flags |= AMQP_BASIC_MESSAGE_ID_FLAG;
    props.message_id =
      amqp_cstring_bytes(properties.message_id.value().c_str());
  }
  if (properties.timestamp.has_value()) {
    props._flags |= AMQP_BASIC_TIMESTAMP_FLAG;
    props.timestamp = properties.timestamp.value();
  }
  if (properties.type.has_value()) {
    props._flags |= AMQP_BASIC_TYPE_FLAG;
    props.type = amqp_cstring_bytes(properties.type.value().c_str());
  }
  if (properties.user_id.has_value()) {
    props._flags |= AMQP_BASIC_USER_ID_FLAG;
    props.user_id = amqp_cstring_bytes(properties.user_id.value().c_str());
  }
  if (properties.app_id.has_value()) {
    props._flags |= AMQP_BASIC_APP_ID_FLAG;
    props.app_id = amqp_cstring_bytes(properties.app_id.value().c_str());
  }
  if (properties.cluster_id.has_value()) {
    props._flags |= AMQP_BASIC_CLUSTER_ID_FLAG;
    props.cluster_id =
      amqp_cstring_bytes(properties.cluster_id.value().c_str());
  }
  return props;
}

static amqp_table_t
convert_to_amqp_table(const AmqpTable& table)
{
  auto convert_to_amqp_field_value = [](const AmqpTableValue& value) {
    amqp_field_value_t v;
    switch (value.getType()) {
      case AmqpTableValue::VT_bool:
        v.kind = AMQP_FIELD_KIND_BOOLEAN;
        v.value.boolean = value.getBool();
        break;
      case AmqpTableValue::VT_int8:
        v.kind = AMQP_FIELD_KIND_I8;
        v.value.i8 = value.getInt8();
        break;
      case AmqpTableValue::VT_int16:
        v.kind = AMQP_FIELD_KIND_I16;
        v.value.i16 = value.getInt16();
        break;
      case AmqpTableValue::VT_int32:
        v.kind = AMQP_FIELD_KIND_I32;
        v.value.i32 = value.getInt32();
        break;
      case AmqpTableValue::VT_int64:
        v.kind = AMQP_FIELD_KIND_I64;
        v.value.i64 = value.getInt64();
        break;
      case AmqpTableValue::VT_float:
        v.kind = AMQP_FIELD_KIND_F32;
        v.value.f32 = value.getFloat();
        break;
      case AmqpTableValue::VT_double:
        v.kind = AMQP_FIELD_KIND_F64;
        v.value.f64 = value.getDouble();
        break;
      case AmqpTableValue::VT_string:
        v.kind = AMQP_FIELD_KIND_UTF8;
        v.value.bytes = amqp_cstring_bytes(value.getString().c_str());
        break;
      case AmqpTableValue::VT_array:
        break;
      case AmqpTableValue::VT_table:
        v.kind = AMQP_FIELD_KIND_TABLE;
        v.value.table = convert_to_amqp_table(value.getTable());
        break;
      case AmqpTableValue::VT_uint8:
        v.kind = AMQP_FIELD_KIND_U8;
        v.value.u8 = value.getUint8();
        break;
      case AmqpTableValue::VT_uint16:
        v.kind = AMQP_FIELD_KIND_U16;
        v.value.u16 = value.getUint16();
        break;
      case AmqpTableValue::VT_uint32:
        v.kind = AMQP_FIELD_KIND_U32;
        v.value.u32 = value.getUint32();
        break;
      case AmqpTableValue::VT_uint64:
        v.kind = AMQP_FIELD_KIND_U64;
        v.value.u64 = value.getUint64();
        break;
    }
    return v;
  };

  amqp_table_t new_table;
  new_table.num_entries = table.size();
  new_table.entries = new amqp_table_entry_t[table.size()];

  amqp_table_entry_t* output_it = new_table.entries;

  for (AmqpTable::const_iterator it = table.begin(); it != table.end();
       ++it, ++output_it) {
    output_it->key = amqp_cstring_bytes(it->first.c_str());
    output_it->value = convert_to_amqp_field_value(it->second);
  }

  return new_table;
}

static void
destroy_amqp_table_entries(amqp_table_t& table)
{
  if (table.num_entries > 0) {
    for (int i = 0; i < table.num_entries; ++i) {
      if (table.entries[i].value.kind == AMQP_FIELD_KIND_TABLE) {
        destroy_amqp_table_entries(table.entries[i].value.value.table);
      }
    }
    delete[] table.entries;
  }
}

static bool
checkConsumeMessageLibErr(int library_error,
                          amqp_connection_state_t& connection,
                          std::string& errorText)
{
  switch (library_error) {
    case AMQP_STATUS_OK:
    case AMQP_STATUS_TIMEOUT:
    case _AMQP_STATUS_NEXT_VALUE:
    case _AMQP_STATUS_TCP_NEXT_VALUE:
    case _AMQP_STATUS_SSL_NEXT_VALUE:
      return true;
    case AMQP_STATUS_UNEXPECTED_STATE: {
      amqp_frame_t frame;
      auto result = amqp_simple_wait_frame(connection, &frame);
      // getLogger()->warn("wait frame res: code: {} error text: {}",
      //                   result,
      //                   amqp_error_string2(result));
    }
      return true;
    default: {
      errorText = amqp_error_string2(library_error);
      // getLogger()->error("AMQP consume message error: code: {} error text:
      // {}",
      //                    library_error,
      //                    errorText);
    }
      return false;
  }
}

using value_t = std::variant<bool,
                             std::int8_t,
                             std::int16_t,
                             std::int32_t,
                             std::int64_t,
                             float,
                             double,
                             std::string,
                             std::vector<AmqpTableValue>,
                             AmqpTable,
                             std::uint8_t,
                             std::uint16_t,
                             std::uint32_t,
                             std::uint64_t>;

struct AmqpTableValue::Impl
{
  value_t m_value;
  Impl(const value_t& v)
    : m_value(v)
  {
  }

  virtual ~Impl() {}
};

AmqpTableValue::AmqpTableValue(const AmqpTableValue& l)
  : m_impl(new Impl(l.m_impl->m_value))
{
}

AmqpTableValue::AmqpTableValue(bool value)
  : m_impl(new Impl(value))
{
}

AmqpTableValue::AmqpTableValue(std::uint8_t value)
  : m_impl(new Impl(value))
{
}

AmqpTableValue::AmqpTableValue(std::int8_t value)
  : m_impl(new Impl(value))
{
}

AmqpTableValue::AmqpTableValue(std::uint16_t value)
  : m_impl(new Impl(value))
{
}

AmqpTableValue::AmqpTableValue(std::int16_t value)
  : m_impl(new Impl(value))
{
}

AmqpTableValue::AmqpTableValue(std::uint32_t value)
  : m_impl(new Impl(value))
{
}

AmqpTableValue::AmqpTableValue(std::int32_t value)
  : m_impl(new Impl(value))
{
}

AmqpTableValue::AmqpTableValue(std::int64_t value)
  : m_impl(new Impl(value))
{
}

AmqpTableValue::AmqpTableValue(std::uint64_t value)
  : m_impl(new Impl(value))
{
}

AmqpTableValue::AmqpTableValue(float value)
  : m_impl(new Impl(value))
{
}

AmqpTableValue::AmqpTableValue(double value)
  : m_impl(new Impl(value))
{
}

AmqpTableValue::AmqpTableValue(const char* value)
  : m_impl(new Impl(value))
{
}

AmqpTableValue::AmqpTableValue(const std::string& value)
  : m_impl(new Impl(value))
{
}

AmqpTableValue::~AmqpTableValue() {}

AmqpTableValue::ValueType
AmqpTableValue::getType() const
{
  return static_cast<ValueType>(m_impl->m_value.index());
}

bool
AmqpTableValue::getBool() const
{
  return std::get<bool>(m_impl->m_value);
}

std::uint8_t
AmqpTableValue::getUint8() const
{
  return std::get<std::uint8_t>(m_impl->m_value);
}

std::int8_t
AmqpTableValue::getInt8() const
{
  return std::get<std::int8_t>(m_impl->m_value);
}

std::uint16_t
AmqpTableValue::getUint16() const
{
  return std::get<std::uint16_t>(m_impl->m_value);
}

std::int16_t
AmqpTableValue::getInt16() const
{
  return std::get<std::int16_t>(m_impl->m_value);
}

std::uint32_t
AmqpTableValue::getUint32() const
{
  return std::get<std::uint32_t>(m_impl->m_value);
}

std::int32_t
AmqpTableValue::getInt32() const
{
  return std::get<std::int32_t>(m_impl->m_value);
}

std::uint64_t
AmqpTableValue::getUint64() const
{
  return std::get<std::uint64_t>(m_impl->m_value);
}

std::int64_t
AmqpTableValue::getInt64() const
{
  return std::get<std::int64_t>(m_impl->m_value);
}

float
AmqpTableValue::getFloat() const
{
  return std::get<float>(m_impl->m_value);
}

double
AmqpTableValue::getDouble() const
{
  return std::get<double>(m_impl->m_value);
}

std::string
AmqpTableValue::getString() const
{
  return std::get<std::string>(m_impl->m_value);
}

std::vector<AmqpTableValue>
AmqpTableValue::getArray() const
{
  return std::get<std::vector<AmqpTableValue>>(m_impl->m_value);
}

AmqpTable
AmqpTableValue::getTable() const
{
  return std::get<AmqpTable>(m_impl->m_value);
}

AmqpMessage::AmqpMessage() {}

AmqpMessage::~AmqpMessage() {}

AmqpEnvelope::AmqpEnvelope(const AmqpMessage& message,
                           const std::string& consumer_tag,
                           const std::uint64_t delivery_tag,
                           const std::string& exchange,
                           bool redelivered,
                           const std::string& routing_key)
  : m_message(message)
  , m_consumerTag(consumer_tag)
  , m_deliveryTag(delivery_tag)
  , m_exchange(exchange)
  , m_redelivered(redelivered)
  , m_routingKey(routing_key)
{
}

AmqpEnvelope::~AmqpEnvelope() {}

struct AmqpConnection::Impl
{
  amqp_connection_state_t state;
};

AmqpConnection::AmqpConnection()
  : m_impl(new Impl)
{
  m_impl->state = amqp_new_connection();
}

AmqpConnection::~AmqpConnection()
{
  if (m_impl->state) {
    die_on_amqp_error(amqp_connection_close(m_impl->state, AMQP_REPLY_SUCCESS),
                      "connection.close");
    die_on_error(amqp_destroy_connection(m_impl->state), "connection.destroy");
  }
}

void
AmqpConnection::open(const std::string& host, int port)
{
  amqp_socket_t* socket = amqp_tcp_socket_new(m_impl->state);
  if (!socket) {
    die("AMQP creating TCP socket failed");
  }
  int status = amqp_socket_open(socket, host.c_str(), port);
  if (status) {
    die("AMQP opening TCP socket on %s:%d failed", host.c_str(), port);
  }
}

void
AmqpConnection::login(const std::string& vhost,
                      const std::string& username,
                      const std::string& password,
                      int frame_max) const
{
  die_on_amqp_error(amqp_login(m_impl->state,
                               vhost.c_str(),
                               0,
                               frame_max,
                               0,
                               AMQP_SASL_METHOD_PLAIN,
                               username.c_str(),
                               password.c_str()),
                    "Logging in");
}

struct AmqpChannel::Impl
{
  amqp_connection_state_t state;
  amqp_channel_t channel;
};

const char* AmqpChannel::EXCHANGE_TYPE_DIRECT = "direct";

const char* AmqpChannel::EXCHANGE_TYPE_FANOUT = "fanout";

const char* AmqpChannel::EXCHANGE_TYPE_TOPIC = "topic";

AmqpChannel::AmqpChannel(const AmqpConnection::Ptr conn)
  : m_impl(new Impl)
{
  /// @todo The amqp_connection_state_t object is not synchronized. In order to
  /// use it from multiple threads you would have to synchronize its use across
  /// threads (provide some external mutual exclusion when calling any amqp_*
  /// function that uses a common amqp_connection_state_t). In order to consume
  /// from multiple channels you need to read messages from all the channels
  /// using amqp_simple_wait_frame, then provide your own logic to handle
  /// messages from different channels appropriately.
  m_impl->state = conn->m_impl->state;
  m_impl->channel = 1u;

  amqp_channel_open(m_impl->state, m_impl->channel);
  die_on_amqp_error(amqp_get_rpc_reply(m_impl->state), "channel.open");
}

AmqpChannel::~AmqpChannel()
{
  die_on_amqp_error(
    amqp_channel_close(m_impl->state, m_impl->channel, AMQP_REPLY_SUCCESS),
    "channel.close");
}

void
AmqpChannel::exchangeDeclare(const std::string& exchange_name,
                             const std::string& exchange_type,
                             bool passive,
                             bool durable,
                             bool auto_delete,
                             bool internal)
{
  amqp_exchange_declare(m_impl->state,
                        m_impl->channel,
                        amqp_cstring_bytes(exchange_name.c_str()),
                        amqp_cstring_bytes(exchange_type.c_str()),
                        passive,
                        durable,
                        auto_delete,
                        internal,
                        amqp_empty_table);
  die_on_amqp_error(amqp_get_rpc_reply(m_impl->state), "exchange.declare");
}

void
AmqpChannel::exchangeDeclare(const std::string& exchange_name,
                             const std::string& exchange_type,
                             bool passive,
                             bool durable,
                             bool auto_delete,
                             bool internal,
                             const AmqpTable& arguments)
{
  amqp_table_t args = convert_to_amqp_table(arguments);
  amqp_exchange_declare(m_impl->state,
                        m_impl->channel,
                        amqp_cstring_bytes(exchange_name.c_str()),
                        amqp_cstring_bytes(exchange_type.c_str()),
                        passive,
                        durable,
                        auto_delete,
                        internal,
                        args);
  die_on_amqp_error(amqp_get_rpc_reply(m_impl->state), "exchange.declare");
  destroy_amqp_table_entries(args);
}

void
AmqpChannel::exchangeBind(const std::string& destination,
                          const std::string& source,
                          const std::string& routing_key)
{
}

void
AmqpChannel::exchangeUnbind(const std::string& destination,
                            const std::string& source,
                            const std::string& routing_key)
{
}

std::string
AmqpChannel::queueDeclare(const std::string& queue_name,
                          bool passive,
                          bool durable,
                          bool exclusive,
                          bool auto_delete)
{
  amqp_queue_declare_ok_t* r =
    amqp_queue_declare(m_impl->state,
                       m_impl->channel,
                       amqp_cstring_bytes(queue_name.c_str()),
                       passive,
                       durable,
                       exclusive,
                       auto_delete,
                       amqp_empty_table);
  die_on_amqp_error(amqp_get_rpc_reply(m_impl->state), "queue.declare");
  return std::string((char*)r->queue.bytes, r->queue.len);
}

std::string
AmqpChannel::queueDeclare(const std::string& queue_name,
                          bool passive,
                          bool durable,
                          bool exclusive,
                          bool auto_delete,
                          const AmqpTable& arguments)
{
  amqp_table_t args = convert_to_amqp_table(arguments);
  amqp_queue_declare_ok_t* r =
    amqp_queue_declare(m_impl->state,
                       m_impl->channel,
                       amqp_cstring_bytes(queue_name.c_str()),
                       passive,
                       durable,
                       exclusive,
                       auto_delete,
                       args);
  die_on_amqp_error(amqp_get_rpc_reply(m_impl->state), "queue.declare");
  destroy_amqp_table_entries(args);
  return std::string((char*)r->queue.bytes, r->queue.len);
}

void
AmqpChannel::queueBind(const std::string& queue_name,
                       const std::string& exchange_name,
                       const std::string& routing_key)
{
  amqp_queue_bind(m_impl->state,
                  m_impl->channel,
                  amqp_cstring_bytes(queue_name.c_str()),
                  amqp_cstring_bytes(exchange_name.c_str()),
                  amqp_cstring_bytes(routing_key.c_str()),
                  amqp_empty_table);
  die_on_amqp_error(amqp_get_rpc_reply(m_impl->state), "queue.bind");
}

void
AmqpChannel::queueUnbind(const std::string& queue_name,
                         const std::string& exchange_name,
                         const std::string& routing_key)
{
}

void
AmqpChannel::basicPublish(const std::string& exchange,
                          const std::string& routing_key,
                          const AmqpMessage& message,
                          bool mandatory,
                          bool immediate)
{
  amqp_basic_properties_t props =
    convert_to_amqp_basic_properties(message.properties());
  die_on_error(amqp_basic_publish(m_impl->state,
                                  m_impl->channel,
                                  amqp_cstring_bytes(exchange.c_str()),
                                  amqp_cstring_bytes(routing_key.c_str()),
                                  mandatory,
                                  immediate,
                                  &props,
                                  amqp_cstring_bytes(message.body().c_str())),
               "basic.publish");
}

std::string
AmqpChannel::basicConsume(const std::string& queue_name,
                          const std::string& consumer_tag,
                          bool no_local,
                          bool no_ack,
                          bool exclusive)
{
  amqp_basic_consume_ok_t* r =
    amqp_basic_consume(m_impl->state,
                       m_impl->channel,
                       amqp_cstring_bytes(queue_name.c_str()),
                       amqp_cstring_bytes(consumer_tag.c_str()),
                       no_local,
                       no_ack,
                       exclusive,
                       amqp_empty_table);
  die_on_amqp_error(amqp_get_rpc_reply(m_impl->state), "basic.consume");
  return std::string((char*)r->consumer_tag.bytes, r->consumer_tag.len);
}

void
AmqpChannel::basicCancel(const std::string& consumer_tag)
{
  if (!consumer_tag.empty()) {
    amqp_basic_cancel(
      m_impl->state, m_impl->channel, amqp_cstring_bytes(consumer_tag.c_str()));
    die_on_amqp_error(amqp_get_rpc_reply(m_impl->state), "basic.cancel");
  }
}

void
AmqpChannel::basicQos(uint32_t prefetch_size,
                      uint16_t prefetch_count,
                      bool global)
{
  if (!amqp_basic_qos(m_impl->state,
                      m_impl->channel,
                      prefetch_count,
                      prefetch_size,
                      global)) {
    die_on_amqp_error(amqp_get_rpc_reply(m_impl->state), "basic.qos");
  }
}

void
AmqpChannel::basicAck(uint64_t delivery_tag, bool multiple)
{
  die_on_error(
    amqp_basic_ack(m_impl->state, m_impl->channel, delivery_tag, multiple),
    "basic.ack");
}

void
AmqpChannel::basicNack(uint64_t delivery_tag, bool multiple, bool requeue)
{
  die_on_error(
    amqp_basic_nack(
      m_impl->state, m_impl->channel, delivery_tag, multiple, requeue),
    "basic.nack");
}

AmqpEnvelope::Ptr
AmqpChannel::basicConsumeMessage(const struct timeval* timeout)
{
  amqp_rpc_reply_t res;
  amqp_envelope_t envelope;

  amqp_maybe_release_buffers(m_impl->state);
  res = amqp_consume_message(m_impl->state, &envelope, timeout, 0);
  std::string errorText{ "unknown" };

  if (AMQP_RESPONSE_NORMAL != res.reply_type) {
    if (!checkConsumeMessageLibErr(res.library_error, m_impl->state, errorText))
      throw std::runtime_error(errorText);
    return nullptr;
  }

  AmqpMessage message;
  message.body() = amqp_bytes_string(envelope.message.body);
  message.properties() =
    convert_to_amqp_properties(envelope.message.properties);

  auto envelope2 =
    AmqpEnvelope::createInstance(message,
                                 amqp_bytes_string(envelope.consumer_tag),
                                 envelope.delivery_tag,
                                 amqp_bytes_string(envelope.exchange),
                                 envelope.redelivered,
                                 amqp_bytes_string(envelope.routing_key));

  amqp_destroy_envelope(&envelope);
  return envelope2;
}

} // end namespace amqp

using namespace gs::amqp;

struct MessageBroker::Impl
{
  std::string host;
  int port;
  std::string username;
  std::string password;
  std::string vhost;
  int frame_max;
  std::vector<std::thread> threads;
  std::atomic<bool> close{ false };
};

MessageBroker::MessageBroker(const std::string& host,
                             int port,
                             const std::string& username,
                             const std::string& password,
                             const std::string& vhost,
                             int frame_max)
  : m_impl(new Impl)
{
  std::srand(std::time(NULL));

  if (host.empty()) {
    throw std::runtime_error("host is not specified, it is required");
  }
  if (vhost.empty()) {
    throw std::runtime_error("vhost is not specified, it is required");
  }
  if (port <= 0) {
    throw std::runtime_error(
      "port is not valm_id, it must be a positive number");
  }

  m_impl->host = host;
  m_impl->port = port;
  m_impl->username = username;
  m_impl->password = password;
  m_impl->vhost = vhost;
  m_impl->frame_max = frame_max;
}

MessageBroker::MessageBroker(const std::string& url, int frame_max)
  : m_impl(new Impl)
{
  std::srand(std::time(NULL));

  struct amqp_connection_info ci;
  char* p = strdup(url.c_str());

  die_on_error(amqp_parse_url(p, &ci), "Parsing URL");

  m_impl->host = ci.host;
  m_impl->port = ci.port;
  m_impl->username = ci.user;
  m_impl->password = ci.password;
  m_impl->vhost = ci.vhost;
  m_impl->frame_max = frame_max;

  free(p);
}

MessageBroker::~MessageBroker()
{
  m_impl->close = true;
  for (auto it = m_impl->threads.begin(); it != m_impl->threads.end(); it++) {
    it->join();
  }
}

const std::string
MessageBroker::generateRandomString()
{
  static const char ALPHABET[] = { "0123456789"
                                   "abcdefgjhiklmnopqrstvwxyz"
                                   "ABCDEFGJHIKLMNOPQRSTVWXYZ" };
  std::string result;
  for (std::size_t i = 0; i < 16; i++) {
    result += ALPHABET[rand() % (sizeof(ALPHABET) - 1)];
  }
  return result;
}

const char* MessageBroker::MESSAGE_TYPE_REQUEST = "request";

const char* MessageBroker::MESSAGE_TYPE_RESPONSE = "response";

const char* MessageBroker::MESSAGE_TYPE_ERROR = "error";

void
MessageBroker::publish(const Configuration& cfg, Message msg)
{
  AmqpConnection::Ptr conn = AmqpConnection::createInstance();
  conn->open(m_impl->host, m_impl->port);
  conn->login(
    m_impl->vhost, m_impl->username, m_impl->password, m_impl->frame_max);

  AmqpChannel::Ptr channel = AmqpChannel::createInstance(conn);
  auto [exchange, queue] = setup(cfg, channel);

  if (!msg.properties().content_type.has_value())
    msg.properties().content_type = "application/json";
  if (!msg.properties().delivery_mode.has_value())
    msg.properties().delivery_mode = 2u;

  channel->basicPublish(exchange, cfg.routing_key, msg);
}

MessageBroker::Response::Ptr
MessageBroker::publish(const Configuration& cfg,
                       Request req,
                       struct timeval* timeout)
{
  auto conn = AmqpConnection::createInstance();
  conn->open(m_impl->host, m_impl->port);
  conn->login(
    m_impl->vhost, m_impl->username, m_impl->password, m_impl->frame_max);

  auto channel = AmqpChannel::createInstance(conn);
  auto [exchange, reply_to] = setup(cfg, channel);

  if (!req.properties().content_type.has_value())
    req.properties().content_type = "application/json";
  if (!req.properties().delivery_mode.has_value())
    req.properties().delivery_mode = 2u;
  if (!req.properties().reply_to.has_value())
    req.properties().reply_to = reply_to;
  if (!req.properties().correlation_id.has_value())
    req.properties().correlation_id = generateRandomString();
  if (!req.properties().type.has_value())
    req.properties().type = MESSAGE_TYPE_REQUEST;

  channel->basicPublish(exchange, cfg.routing_key, req);
  channel->basicConsume(reply_to);

  struct timeval tv = { 30, 0 };
  Response::Ptr res;

  for (;;) {
    auto envelope = channel->basicConsumeMessage(timeout ? timeout : &tv);
    if (!envelope)
      return nullptr;
    res = Response::createInstance();
    res->body() = envelope->message().body();
    res->properties() = envelope->message().properties();
    break;
  }

  return res;
}

void
MessageBroker::subscribe(const Configuration& cfg,
                         std::function<void(const Message&)> callback)
{
  std::thread worker([this, cfg, callback]() {
    struct timeval tv = { 1, 0 };
    auto conn = AmqpConnection::createInstance();
    conn->open(m_impl->host, m_impl->port);
    conn->login(
      m_impl->vhost, m_impl->username, m_impl->password, m_impl->frame_max);

    auto channel = AmqpChannel::createInstance(conn);
    auto [exchange, queue] = setup(cfg, channel);
    channel->basicConsume(queue);

    while (!m_impl->close) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      auto envelope = channel->basicConsumeMessage(&tv);
      if (!envelope) {
        continue;
      }
      callback(envelope->message());
    }
  });

  m_impl->threads.push_back(std::move(worker));
}

void
MessageBroker::subscribe(
  const Configuration& cfg,
  std::function<bool(const Request&, Response&)> callback)
{
  std::thread worker([this, cfg, callback]() {
    struct timeval tv = { 1, 0 };
    auto conn = AmqpConnection::createInstance();
    conn->open(m_impl->host, m_impl->port);
    conn->login(
      m_impl->vhost, m_impl->username, m_impl->password, m_impl->frame_max);

    auto channel = AmqpChannel::createInstance(conn);
    auto [exchange, queue] = setup(cfg, channel);
    channel->basicConsume(queue);

    while (!m_impl->close) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      auto envelope = channel->basicConsumeMessage(&tv);
      if (!envelope) {
        continue;
      }

      Request req;
      req.body() = envelope->message().body();
      req.properties() = envelope->message().properties();
      Response res;

      auto ok = callback(req, res);
      std::string reply_to(req.properties().reply_to.value());
      std::string correlation_id(req.properties().correlation_id.value());

      if (!res.properties().content_type.has_value())
        res.properties().content_type = "application/json";
      if (!res.properties().delivery_mode.has_value())
        res.properties().delivery_mode = 2u;
      if (!res.properties().correlation_id.has_value())
        res.properties().correlation_id = correlation_id;
      if (!res.properties().type.has_value())
        res.properties().type = ok ? MESSAGE_TYPE_RESPONSE : MESSAGE_TYPE_ERROR;

      channel->basicPublish("", reply_to, res);
    }
  });

  m_impl->threads.push_back(std::move(worker));
}

void
MessageBroker::close()
{
  m_impl->close = true;
}

std::tuple<std::string, std::string>
MessageBroker::setup(const Configuration& cfg, AmqpChannel::Ptr channel)
{
  std::string exchange_name, queue_name;

  if (cfg.exchange.name == "amq") {
    exchange_name = "amq." + cfg.exchange.type;
  }

  if (cfg.exchange.declare) {
    if (cfg.exchange.arguments.has_value()) {
      channel->exchangeDeclare(cfg.exchange.name,
                               cfg.exchange.type,
                               cfg.exchange.passive,
                               cfg.exchange.durable,
                               cfg.exchange.auto_delete,
                               cfg.exchange.internal,
                               cfg.exchange.arguments.value());
    } else {
      channel->exchangeDeclare(cfg.exchange.name,
                               cfg.exchange.type,
                               cfg.exchange.passive,
                               cfg.exchange.durable,
                               cfg.exchange.auto_delete,
                               cfg.exchange.internal);
    }
    exchange_name = cfg.exchange.name;
  }

  if (cfg.queue.declare) {
    if (cfg.queue.arguments.has_value()) {
      queue_name = channel->queueDeclare(cfg.queue.name,
                                         cfg.queue.passive,
                                         cfg.queue.durable,
                                         cfg.queue.exclusive,
                                         cfg.queue.auto_delete,
                                         cfg.queue.arguments.value());
    } else {
      queue_name = channel->queueDeclare(cfg.queue.name,
                                         cfg.queue.passive,
                                         cfg.queue.durable,
                                         cfg.queue.exclusive,
                                         cfg.queue.auto_delete);
    }
  }

  if (cfg.queue.bind) {
    std::string binding_key =
      cfg.exchange.type == AmqpChannel::EXCHANGE_TYPE_TOPIC
        ? cfg.routing_pattern
        : cfg.routing_key;
    channel->queueBind(queue_name, exchange_name, binding_key);
  }

  return std::make_tuple(exchange_name, queue_name);
}

} // end namespace gs
