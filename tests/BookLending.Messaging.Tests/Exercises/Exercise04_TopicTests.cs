using Azure.Messaging.ServiceBus;
using BookLending.Messaging.Application.Messaging;
using BookLending.Messaging.Domain.Events;
using BookLending.Messaging.Infrastructure.ServiceBus;
using BookLending.Messaging.Tests.Support;

namespace BookLending.Messaging.Tests.Exercises;

[Collection(ServiceBusCollection.Name)]
public sealed class Exercise04_TopicTests(ServiceBusFixture fx) : IAsyncLifetime
{
    private const string Topic = EntityNames.BookEventsTopic;

    public async Task InitializeAsync()
    {
        await fx.PurgeSubscriptionAsync(Topic, EntityNames.EmailNotificationsSubscription);
        await fx.PurgeSubscriptionAsync(Topic, EntityNames.OverdueAlertsSubscription);
        await fx.PurgeSubscriptionAsync(Topic, EntityNames.FictionAnalyticsSubscription);
    }

    public Task DisposeAsync() => Task.CompletedTask;

    [Fact]
    public async Task Each_subscription_gets_only_what_its_filter_matches()
    {
        await using var broadcaster = new ServiceBusBookEventBroadcaster(fx.Client);
        var fictionOnTime = Sample.Returned("Fiction", 0);
        var fictionLate = Sample.Returned("Fiction", 3);
        var historyLate = Sample.Returned("History", 5);
        var historyOnTime = Sample.Returned("History", 0);

        foreach (var evt in new[] { fictionOnTime, fictionLate, historyLate, historyOnTime })
        {
            await broadcaster.BroadcastReturnedAsync(evt);
        }

        var email = await ReceiveIdsAsync(EntityNames.EmailNotificationsSubscription, 4);
        var overdue = await ReceiveIdsAsync(EntityNames.OverdueAlertsSubscription, 2);
        var fiction = await ReceiveIdsAsync(EntityNames.FictionAnalyticsSubscription, 2);

        Assert.Equal(Ids(fictionOnTime, fictionLate, historyLate, historyOnTime), email);
        Assert.Equal(Ids(fictionLate, historyLate), overdue);
        Assert.Equal(Ids(fictionOnTime, fictionLate), fiction);
    }

    [Fact]
    public async Task Routing_properties_and_body_are_set_correctly()
    {
        await using var broadcaster = new ServiceBusBookEventBroadcaster(fx.Client);
        var evt = Sample.Returned("Fiction", 7);

        await broadcaster.BroadcastReturnedAsync(evt);

        await using var receiver = fx.Client.CreateReceiver(Topic, EntityNames.EmailNotificationsSubscription,
            new ServiceBusReceiverOptions { ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete });
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));

        Assert.NotNull(msg);
        Assert.Equal("BookReturned", msg.Subject);
        Assert.Equal("application/json", msg.ContentType);
        Assert.Equal(evt.EventId.ToString(), msg.MessageId);
        Assert.Equal("Fiction", msg.ApplicationProperties["category"]);
        Assert.IsType<int>(msg.ApplicationProperties["daysOverdue"]);
        Assert.Equal(7, msg.ApplicationProperties["daysOverdue"]);
        Assert.Equal(evt, Sample.Deserialize<BookReturned>(msg));
    }

    private async Task<SortedSet<string>> ReceiveIdsAsync(string subscription, int expected)
    {
        await using var receiver = fx.Client.CreateReceiver(Topic, subscription,
            new ServiceBusReceiverOptions { ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete });
        var messages = await fx.ReceiveAllAsync(receiver, expected, TimeSpan.FromSeconds(15));
        return new SortedSet<string>(messages.Select(m => m.MessageId));
    }

    private static SortedSet<string> Ids(params BookReturned[] events) =>
        new(events.Select(e => e.EventId.ToString()));
}
