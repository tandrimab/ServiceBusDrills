using Azure.Messaging.ServiceBus;
using BookLending.Messaging.Application.Abstractions;
using BookLending.Messaging.Application.Messaging;
using BookLending.Messaging.Infrastructure.ServiceBus;
using BookLending.Messaging.Tests.Support;
using Microsoft.Extensions.Logging.Abstractions;

namespace BookLending.Messaging.Tests.Exercises;

[Collection(ServiceBusCollection.Name)]
public sealed class Exercise03_FailureTests(ServiceBusFixture fx) : IAsyncLifetime
{
    private const string Queue = EntityNames.LoanEventsQueue;
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(20);

    public async Task InitializeAsync()
    {
        await fx.PurgeQueueAsync(Queue);
        await fx.PurgeDeadLetterAsync(Queue);
    }

    public Task DisposeAsync() => Task.CompletedTask;

    [Fact]
    public async Task Poison_message_is_dead_lettered_immediately_with_reason_and_description()
    {
        var handler = new RecordingLoanEventHandler
        {
            Behaviour = _ => throw new PoisonMessageException("Due date is in the past"),
        };
        var evt = Sample.Borrowed();

        var dead = await RunAndWaitForDeadLetterAsync(handler, Sample.AsMessage(evt));

        Assert.Equal(evt.EventId.ToString(), dead.MessageId);
        Assert.Equal(DeadLetterReasons.PoisonMessage, dead.Reason);
        Assert.Equal("Due date is in the past", dead.Description);
        Assert.Equal(1, handler.Calls); // no pointless retries
    }

    [Fact]
    public async Task Unknown_subject_is_dead_lettered_without_calling_the_handler()
    {
        var handler = new RecordingLoanEventHandler();
        var msg = new ServiceBusMessage("{}") { Subject = "BookBurned", ContentType = "application/json" };

        var dead = await RunAndWaitForDeadLetterAsync(handler, msg);

        Assert.Equal(DeadLetterReasons.UnknownEventType, dead.Reason);
        Assert.Equal(0, handler.Calls);
    }

    [Fact]
    public async Task Malformed_body_is_dead_lettered_without_calling_the_handler()
    {
        var handler = new RecordingLoanEventHandler();
        var msg = new ServiceBusMessage("this is not json") { Subject = "BookBorrowed", ContentType = "application/json" };

        var dead = await RunAndWaitForDeadLetterAsync(handler, msg);

        Assert.Equal(DeadLetterReasons.DeserializationFailed, dead.Reason);
        Assert.Equal(0, handler.Calls);
    }

    [Fact]
    public async Task Transient_failure_is_retried_until_MaxDeliveryCount_then_service_dead_letters_it()
    {
        var handler = new RecordingLoanEventHandler
        {
            Behaviour = _ => throw new TimeoutException("Database is having a moment"),
        };

        var dead = await RunAndWaitForDeadLetterAsync(handler, Sample.AsMessage(Sample.Borrowed()));

        // This reason is set by Service Bus itself, not by your code.
        Assert.Equal("MaxDeliveryCountExceeded", dead.Reason);
        Assert.Equal(3, handler.Calls);
    }

    [Fact]
    public async Task Transient_failure_that_recovers_is_completed_normally()
    {
        var attempts = 0;
        var handler = new RecordingLoanEventHandler
        {
            Behaviour = _ => Interlocked.Increment(ref attempts) == 1
                ? throw new TimeoutException("first try fails")
                : Task.CompletedTask,
        };
        await using var processor = new LoanEventsProcessor(fx.Client, handler, NullLogger<LoanEventsProcessor>.Instance);
        await using (var sender = fx.Client.CreateSender(Queue))
        {
            await sender.SendMessageAsync(Sample.AsMessage(Sample.Borrowed()));
        }

        await processor.StartAsync(CancellationToken.None);
        var handled = await Eventually.TrueAsync(() => handler.Handled.Count == 1, Timeout);
        await processor.StopAsync(CancellationToken.None);

        Assert.True(handled, "The message should succeed on the second delivery (did you abandon it?).");
        Assert.Equal(2, handler.Calls);
        Assert.Empty(await new ServiceBusDeadLetterInspector(fx.Client).PeekAsync());
    }

    [Fact]
    public async Task Inspector_peeks_without_removing()
    {
        var handler = new RecordingLoanEventHandler { Behaviour = _ => throw new PoisonMessageException("bad") };
        await RunAndWaitForDeadLetterAsync(handler, Sample.AsMessage(Sample.Borrowed()));
        var inspector = new ServiceBusDeadLetterInspector(fx.Client);

        var first = await inspector.PeekAsync();
        var second = await inspector.PeekAsync();

        Assert.Single(first);
        Assert.Single(second);
        Assert.Equal(first[0].MessageId, second[0].MessageId);
    }

    private async Task<DeadLetteredMessage> RunAndWaitForDeadLetterAsync(RecordingLoanEventHandler handler, ServiceBusMessage message)
    {
        await using var processor = new LoanEventsProcessor(fx.Client, handler, NullLogger<LoanEventsProcessor>.Instance);
        await using (var sender = fx.Client.CreateSender(Queue))
        {
            await sender.SendMessageAsync(message);
        }
        var inspector = new ServiceBusDeadLetterInspector(fx.Client);

        await processor.StartAsync(CancellationToken.None);
        var dead = await Eventually.ValueAsync(() => inspector.PeekAsync(), list => list.Count > 0, Timeout);
        await Task.Delay(1000); // catch any extra (wrong) handler calls
        await processor.StopAsync(CancellationToken.None);

        Assert.True(dead.Count == 1, $"Expected exactly 1 dead-lettered message, found {dead.Count}.");
        return dead[0];
    }
}
