using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Options;
using OpenTelemetry;
using OpenTelemetry.Context.Propagation;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace BeanNotifs.Messaging;

public record ConsumerBinding(string QueueName, string RoutingKey, Func<EventEnvelope, Task> Handler);

// Bounded retry without a dead-letter exchange: on handler failure the message is
// ack'd (removed) and manually re-published to the same routing key with an
// incremented x-retry-count header. After MaxDeliveryAttempts it's logged and
// dropped instead of re-published. The count lives in the message itself, so it
// survives a pod restart (unlike an in-memory counter keyed by delivery tag).
public class EventConsumerHostedService : BackgroundService
{
    private const int MaxDeliveryAttempts = 5;
    private const string RetryCountHeader = "x-retry-count";

    // otel-tracing plan Step 3: manual propagation (see OrderBeans'/PayBeancoins'
    // EventPublisher.cs for why). Notifs has no EventPublisher -- this is the
    // chain's end, consume-only, so this is the only ActivitySource it needs.
    private static readonly ActivitySource ActivitySource = new("BeanNotifs");

    private readonly IConnection _connection;
    private readonly IModel _channel;
    private readonly IReadOnlyList<ConsumerBinding> _bindings;
    private readonly ILogger<EventConsumerHostedService> _logger;

    // Idempotency guard: a redelivery of a message we've already fully handled (e.g.
    // a pod crash between the handler succeeding and the ack landing, or the retry
    // mechanism below re-publishing after a transient failure) must not re-run the
    // handler. In-memory only and never evicted — acceptable at this project's demo
    // scale (message-contracts-hardening plan Q3/10_target_shape non-goals).
    private readonly ConcurrentDictionary<Guid, byte> _processedEventIds = new();

    public EventConsumerHostedService(
        IOptions<RabbitMqOptions> options,
        IReadOnlyList<ConsumerBinding> bindings,
        ILogger<EventConsumerHostedService> logger)
    {
        _bindings = bindings;
        _logger = logger;

        var o = options.Value;
        var factory = new ConnectionFactory
        {
            HostName = o.Host,
            Port = o.Port,
            UserName = o.User,
            Password = o.Pass,
            AutomaticRecoveryEnabled = true,
            DispatchConsumersAsync = true, // required for AsyncEventingBasicConsumer's Received handler to actually be awaited
        };

        _connection = factory.CreateConnection("BeanNotifs-consumer");
        _channel = _connection.CreateModel();
        _channel.ExchangeDeclare(RabbitMqOptions.ExchangeName, ExchangeType.Topic, durable: true);
        _channel.BasicQos(prefetchSize: 0, prefetchCount: 10, global: false);

        foreach (var binding in _bindings)
        {
            _channel.QueueDeclare(binding.QueueName, durable: true, exclusive: false, autoDelete: false);
            _channel.QueueBind(binding.QueueName, RabbitMqOptions.ExchangeName, binding.RoutingKey);
        }
    }

    // No EventPublisher exists in this service (consumer-only, per Q1), so the
    // readiness check probes this connection directly instead.
    public bool IsConnected => _connection.IsOpen && _channel.IsOpen;

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        foreach (var binding in _bindings)
        {
            var consumer = new AsyncEventingBasicConsumer(_channel);
            consumer.Received += (_, ea) => HandleDeliveryAsync(binding, ea);
            _channel.BasicConsume(binding.QueueName, autoAck: false, consumer: consumer);
        }

        return Task.CompletedTask;
    }

    private async Task HandleDeliveryAsync(ConsumerBinding binding, BasicDeliverEventArgs ea)
    {
        EventEnvelope envelope;
        try
        {
            envelope = JsonSerializer.Deserialize<EventEnvelope>(ea.Body.Span)!;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Malformed message on {Queue}, dropping", binding.QueueName);
            _channel.BasicAck(ea.DeliveryTag, multiple: false);
            return;
        }

        if (_processedEventIds.ContainsKey(envelope.EventId))
        {
            _logger.LogInformation(
                "Duplicate delivery of {EventType} (order {OrderId}, event {EventId}) on {Queue} — already processed, skipping",
                envelope.EventType, envelope.OrderId, envelope.EventId, binding.QueueName);
            _channel.BasicAck(ea.DeliveryTag, multiple: false);
            return;
        }

        // otel-tracing plan Step 3: extract the traceparent Pay's/Cooker's
        // EventPublisher injected into the AMQP headers, so this consume span links
        // back to the originating trace instead of starting a disconnected one.
        var parentContext = Propagators.DefaultTextMapPropagator.Extract(
            default, ea.BasicProperties.Headers, ExtractTraceHeaderValue);
        Baggage.Current = parentContext.Baggage;

        using var activity = ActivitySource.StartActivity(
            $"consume {binding.RoutingKey}", ActivityKind.Consumer, parentContext.ActivityContext);
        activity?.SetTag("messaging.system", "rabbitmq");
        activity?.SetTag("messaging.destination", binding.RoutingKey);
        activity?.SetTag("order.id", envelope.OrderId);

        try
        {
            await binding.Handler(envelope);
            _processedEventIds[envelope.EventId] = 0;
            _channel.BasicAck(ea.DeliveryTag, multiple: false);
        }
        catch (Exception ex)
        {
            var attempt = GetRetryCount(ea.BasicProperties) + 1;
            _channel.BasicAck(ea.DeliveryTag, multiple: false); // original delivery is done either way

            if (attempt >= MaxDeliveryAttempts)
            {
                _logger.LogError(ex,
                    "Handler failed {Attempt} times for {EventType} (order {OrderId}) on {Queue} — dropping",
                    attempt, envelope.EventType, envelope.OrderId, binding.QueueName);
                return;
            }

            _logger.LogWarning(ex,
                "Handler failed (attempt {Attempt}/{Max}) for {EventType} (order {OrderId}) on {Queue} — requeueing",
                attempt, MaxDeliveryAttempts, envelope.EventType, envelope.OrderId, binding.QueueName);

            var props = _channel.CreateBasicProperties();
            props.Persistent = true;
            props.ContentType = "application/json";
            props.Headers = new Dictionary<string, object> { [RetryCountHeader] = attempt };

            _channel.BasicPublish(RabbitMqOptions.ExchangeName, binding.RoutingKey, props, ea.Body);
        }
    }

    private static IEnumerable<string> ExtractTraceHeaderValue(IDictionary<string, object>? headers, string key)
    {
        if (headers is not null && headers.TryGetValue(key, out var value) && value is byte[] bytes)
        {
            return new[] { Encoding.UTF8.GetString(bytes) };
        }

        return Enumerable.Empty<string>();
    }

    private static int GetRetryCount(IBasicProperties properties)
    {
        if (properties.Headers is not null && properties.Headers.TryGetValue(RetryCountHeader, out var value))
        {
            return value switch
            {
                int i => i,
                long l => (int)l,
                byte[] bytes => int.Parse(Encoding.UTF8.GetString(bytes)),
                _ => 0,
            };
        }

        return 0;
    }

    public override void Dispose()
    {
        _channel.Dispose();
        _connection.Dispose();
        base.Dispose();
        GC.SuppressFinalize(this);
    }
}
