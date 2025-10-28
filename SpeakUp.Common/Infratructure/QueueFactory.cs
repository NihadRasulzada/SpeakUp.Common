using System.Text;
using System.Text.Json;
using Microsoft.EntityFrameworkCore.Metadata;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace SpeakUp.Common.Infratructure;

public static class QueueFactory
{
    public static async Task SendMessageToExchange(string exchangeName,
        string exchangeType,
        string queueName,
        object obj)
    {
        var channel =
            (await (await (await CreateBasicConsumerAsync()).EnsureExchangeAsync(exchangeName: exchangeName,
                    exchangeType: exchangeType))
                .EnsureQueueAsync(queueName: queueName, exchangeName: exchangeName)).Channel;

        var body = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(obj));

        // await channel.BasicPublishAsync(exchange: exchangeName,
        //     routingKey: queueName,
        //     basicProperties: null,
        //     body: body);

        await channel.BasicPublishAsync(exchangeName, queueName, body);
    }

    public static async Task<AsyncEventingBasicConsumer> CreateBasicConsumerAsync()
    {
        var factory = new ConnectionFactory() { HostName = "localhost", Port = 5672, UserName = "sa", Password = "1234567890Aa"};
        var connection = await factory.CreateConnectionAsync();
        var channel = await connection.CreateChannelAsync();

        return new AsyncEventingBasicConsumer(channel);
    }

    public static async Task<AsyncEventingBasicConsumer> EnsureExchangeAsync(this AsyncEventingBasicConsumer consumer,
        string exchangeName,
        string exchangeType = SpeakUpConstants.DefaultExchangeType)
    {
        await consumer.Channel.ExchangeDeclareAsync(exchange: exchangeName,
            type: exchangeType,
            durable: false,
            autoDelete: false);
        return consumer;
    }

    public static async Task<AsyncEventingBasicConsumer> EnsureQueueAsync(this AsyncEventingBasicConsumer consumer,
        string queueName,
        string exchangeName)
    {
        await consumer.Channel.QueueDeclareAsync(queue: queueName,
            durable: false,
            exclusive: false,
            autoDelete: false,
            null);

        await consumer.Channel.QueueBindAsync(queueName, exchangeName, queueName);

        return consumer;
    }

    public static AsyncEventingBasicConsumer Receive<T>(this AsyncEventingBasicConsumer consumer,
        Action<T> act)
    {
        consumer.ReceivedAsync += async (m, eventArgs) =>
        {
            var body = eventArgs.Body.ToArray();
            var message = Encoding.UTF8.GetString(body);

            var model = JsonSerializer.Deserialize<T>(message);

            act(model);
            await consumer.Channel.BasicAckAsync(eventArgs.DeliveryTag, false);
        };

        return consumer;
    }

    public static async Task<AsyncEventingBasicConsumer> StartConsumingAsync(this AsyncEventingBasicConsumer consumer,
        string queueName)
    {
        await consumer.Channel.BasicConsumeAsync(queue: queueName,
            autoAck: false,
            consumer: consumer);

        return consumer;
    }
}