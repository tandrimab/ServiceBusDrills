using Azure.Messaging.ServiceBus;
using BookLending.Messaging.Application.Messaging;
using BookLending.Messaging.Domain.Events;
using BookLending.Messaging.Infrastructure.ServiceBus;
using BookLending.Messaging.Tests.Support;

namespace BookLending.Messaging.Tests.Exercises;

[Collection(ServiceBusCollection.Name)]
public sealed class Exercise05_BatchTests(ServiceBusFixture fx) : IAsyncLifetime
{
    public Task InitializeAsync() => fx.PurgeQueueAsync(EntityNames.LoanEventsQueue);
    public Task DisposeAsync() => Task.CompletedTask;

    [Fact]
    public async Task Empty_list_sends_nothing_and_returns_zero()
    {
        await using var publisher = new ServiceBusLoanEventPublisher(fx.Client);

        var batches = await publisher.PublishBatchAsync(Array.Empty<BookBorrowed>());

        Assert.Equal(0, batches);
        await using var peeker = fx.Client.CreateReceiver(EntityNames.LoanEventsQueue);
        Assert.Empty(await peeker.PeekMessagesAsync(1));
    }

    [Fact]
    public async Task Large_list_is_split_into_several_batches_and_every_event_arrives_once()
    {
        await using var publisher = new ServiceBusLoanEventPublisher(fx.Client);
        var events = Enumerable.Range(0, 3000).Select(_ => Sample.Borrowed()).ToList();

        var batches = await publisher.PublishBatchAsync(events);

        Assert.True(batches >= 2, $"3,000 events can't fit in one batch, but you reported {batches}.");

        await using var receiver = fx.Client.CreateReceiver(EntityNames.LoanEventsQueue,
            new ServiceBusReceiverOptions { ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete });
        var received = await fx.ReceiveAllAsync(receiver, events.Count, TimeSpan.FromSeconds(90));

        Assert.Equal(events.Count, received.Count);
        Assert.Equal(
            events.Select(e => e.EventId.ToString()).Order(),
            received.Select(m => m.MessageId).Order());
        Assert.All(received, m => Assert.Equal("BookBorrowed", m.Subject));
    }
}
