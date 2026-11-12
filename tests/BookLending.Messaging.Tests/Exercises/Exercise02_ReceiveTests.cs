using BookLending.Messaging.Application.Messaging;
using BookLending.Messaging.Domain.Events;
using BookLending.Messaging.Infrastructure.ServiceBus;
using BookLending.Messaging.Tests.Support;
using Microsoft.Extensions.Logging.Abstractions;

namespace BookLending.Messaging.Tests.Exercises;

[Collection(ServiceBusCollection.Name)]
public sealed class Exercise02_ReceiveTests(ServiceBusFixture fx) : IAsyncLifetime
{
    private const string Queue = EntityNames.LoanEventsQueue;

    public async Task InitializeAsync()
    {
        await fx.PurgeQueueAsync(Queue);
        await fx.PurgeDeadLetterAsync(Queue);
    }

    public Task DisposeAsync() => Task.CompletedTask;

    [Fact]
    public async Task Handles_each_message_as_its_concrete_type_and_removes_it_from_the_queue()
    {
        var handler = new RecordingLoanEventHandler();
        await using var processor = new LoanEventsProcessor(fx.Client, handler, NullLogger<LoanEventsProcessor>.Instance);
        var borrowed = Sample.Borrowed();
        var returned = Sample.Returned("History", 1);

        await using (var sender = fx.Client.CreateSender(Queue))
        {
            await sender.SendMessageAsync(Sample.AsMessage(borrowed));
            await sender.SendMessageAsync(Sample.AsMessage(returned));
        }

        await processor.StartAsync(CancellationToken.None);
        var handledBoth = await Eventually.TrueAsync(() => handler.Handled.Count == 2, TimeSpan.FromSeconds(15));
        await processor.StopAsync(CancellationToken.None);

        Assert.True(handledBoth, $"Expected 2 handled events, got {handler.Handled.Count}.");
        Assert.Contains(handler.Handled, e => e is BookBorrowed b && b == borrowed);
        Assert.Contains(handler.Handled, e => e is BookReturned r && r == returned);

        // Completed messages are gone. Locked-but-not-completed ones would still be peekable.
        await using var peeker = fx.Client.CreateReceiver(Queue);
        Assert.Empty(await peeker.PeekMessagesAsync(10));
        Assert.Equal(2, handler.Calls);
    }

    [Fact]
    public async Task After_StopAsync_new_messages_stay_in_the_queue()
    {
        var handler = new RecordingLoanEventHandler();
        await using var processor = new LoanEventsProcessor(fx.Client, handler, NullLogger<LoanEventsProcessor>.Instance);

        await processor.StartAsync(CancellationToken.None);
        await processor.StopAsync(CancellationToken.None);

        await using (var sender = fx.Client.CreateSender(Queue))
        {
            await sender.SendMessageAsync(Sample.AsMessage(Sample.Borrowed()));
        }
        await Task.Delay(TimeSpan.FromSeconds(3));

        Assert.Equal(0, handler.Calls);
        await using var peeker = fx.Client.CreateReceiver(Queue);
        Assert.Single(await peeker.PeekMessagesAsync(10));
    }
}
