using Azure.Messaging.ServiceBus;
using BookLending.Messaging.Application.Messaging;
using BookLending.Messaging.Domain.Events;
using BookLending.Messaging.Infrastructure.ServiceBus;
using BookLending.Messaging.Tests.Support;

namespace BookLending.Messaging.Tests.Exercises;

[Collection(ServiceBusCollection.Name)]
public sealed class Exercise01_SendTests(ServiceBusFixture fx) : IAsyncLifetime
{
    public Task InitializeAsync() => fx.PurgeQueueAsync(EntityNames.LoanEventsQueue);
    public Task DisposeAsync() => Task.CompletedTask;

    [Fact]
    public async Task Sends_event_with_expected_metadata_and_full_json_body()
    {
        await using var publisher = new ServiceBusLoanEventPublisher(fx.Client);
        var evt = Sample.Borrowed();

        await publisher.PublishAsync(evt);

        var msg = await ReceiveOneAsync();
        Assert.NotNull(msg);
        Assert.Equal("application/json", msg.ContentType);
        Assert.Equal("BookBorrowed", msg.Subject);
        Assert.Equal(evt.EventId.ToString(), msg.MessageId);
        Assert.Equal(evt.LoanId.ToString(), msg.CorrelationId);
        Assert.Equal(evt, Sample.Deserialize<BookBorrowed>(msg));
    }

    [Fact]
    public async Task Serializes_the_concrete_type_even_when_called_through_the_base_type()
    {
        await using var publisher = new ServiceBusLoanEventPublisher(fx.Client);
        LoanEvent evt = Sample.Returned(category: "History", daysOverdue: 2);

        await publisher.PublishAsync(evt);

        var msg = await ReceiveOneAsync();
        Assert.NotNull(msg);
        Assert.Equal("BookReturned", msg.Subject);
        var body = Sample.Deserialize<BookReturned>(msg);
        Assert.Equal("History", body.Category);   // fails if only base-class props were serialized
        Assert.Equal(2, body.DaysOverdue);
    }

    private async Task<ServiceBusReceivedMessage?> ReceiveOneAsync()
    {
        await using var receiver = fx.Client.CreateReceiver(EntityNames.LoanEventsQueue,
            new ServiceBusReceiverOptions { ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete });
        return await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
    }
}
