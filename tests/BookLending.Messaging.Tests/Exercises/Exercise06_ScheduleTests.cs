using Azure.Messaging.ServiceBus;
using BookLending.Messaging.Application.Messaging;
using BookLending.Messaging.Domain.Events;
using BookLending.Messaging.Infrastructure.ServiceBus;
using BookLending.Messaging.Tests.Support;

namespace BookLending.Messaging.Tests.Exercises;

[Collection(ServiceBusCollection.Name)]
public sealed class Exercise06_ScheduleTests(ServiceBusFixture fx) : IAsyncLifetime
{
    private const string Queue = EntityNames.DueRemindersQueue;

    public Task InitializeAsync() => fx.PurgeQueueAsync(Queue);
    public Task DisposeAsync() => Task.CompletedTask;

    [Fact]
    public async Task Reminder_is_invisible_until_its_time_then_arrives()
    {
        await using var scheduler = new ServiceBusReminderScheduler(fx.Client);
        var reminder = NewReminder();
        var deliverAt = DateTimeOffset.UtcNow.AddSeconds(6);

        var sequenceNumber = await scheduler.ScheduleAsync(reminder, deliverAt);

        Assert.True(sequenceNumber > 0, "Return the sequence number Service Bus gave the scheduled message.");
        await using var receiver = fx.Client.CreateReceiver(Queue,
            new ServiceBusReceiverOptions { ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete });

        var early = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2));
        Assert.Null(early);

        var late = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(20));
        Assert.NotNull(late);
        Assert.InRange(late.ScheduledEnqueueTime, deliverAt.AddSeconds(-1), deliverAt.AddSeconds(1));
        Assert.Equal(reminder, Sample.Deserialize<DueDateReminder>(late));
    }

    [Fact]
    public async Task Cancelled_reminder_never_arrives()
    {
        await using var scheduler = new ServiceBusReminderScheduler(fx.Client);

        var sequenceNumber = await scheduler.ScheduleAsync(NewReminder(), DateTimeOffset.UtcNow.AddSeconds(3));
        await scheduler.CancelAsync(sequenceNumber);

        await using var receiver = fx.Client.CreateReceiver(Queue,
            new ServiceBusReceiverOptions { ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete });
        Assert.Null(await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(8)));
    }

    private static DueDateReminder NewReminder() =>
        new(Guid.NewGuid(), Guid.NewGuid(), "Clean Architecture", new DateOnly(2026, 12, 1));
}
