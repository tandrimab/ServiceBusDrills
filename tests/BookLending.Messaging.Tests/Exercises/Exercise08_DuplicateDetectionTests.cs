using Azure.Messaging.ServiceBus;
using BookLending.Messaging.Application.Messaging;
using BookLending.Messaging.Domain.Events;
using BookLending.Messaging.Infrastructure.ServiceBus;
using BookLending.Messaging.Tests.Support;

namespace BookLending.Messaging.Tests.Exercises;

[Collection(ServiceBusCollection.Name)]
public sealed class Exercise08_DuplicateDetectionTests(ServiceBusFixture fx) : IAsyncLifetime
{
    private const string Queue = EntityNames.FinesQueue;

    public Task InitializeAsync() => fx.PurgeQueueAsync(Queue);
    public Task DisposeAsync() => Task.CompletedTask;

    [Fact]
    public async Task Publishing_the_same_fine_twice_stores_it_once()
    {
        await using var publisher = new ServiceBusFinePublisher(fx.Client);
        var fine = new FineIssued(Guid.NewGuid(), Guid.NewGuid(), 50m);

        await publisher.PublishAsync(fine);
        await publisher.PublishAsync(fine); // the "retry"

        var received = await ReceiveAsync(expected: 1);
        var msg = Assert.Single(received);
        Assert.Equal(fine.FineId.ToString(), msg.MessageId);
        Assert.Equal(fine, Sample.Deserialize<FineIssued>(msg));
    }

    [Fact]
    public async Task Different_fines_are_both_kept()
    {
        await using var publisher = new ServiceBusFinePublisher(fx.Client);

        await publisher.PublishAsync(new FineIssued(Guid.NewGuid(), Guid.NewGuid(), 10m));
        await publisher.PublishAsync(new FineIssued(Guid.NewGuid(), Guid.NewGuid(), 10m));

        Assert.Equal(2, (await ReceiveAsync(expected: 2)).Count);
    }

    private async Task<List<ServiceBusReceivedMessage>> ReceiveAsync(int expected)
    {
        await using var receiver = fx.Client.CreateReceiver(Queue,
            new ServiceBusReceiverOptions { ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete });
        return await fx.ReceiveAllAsync(receiver, expected, TimeSpan.FromSeconds(10));
    }
}
