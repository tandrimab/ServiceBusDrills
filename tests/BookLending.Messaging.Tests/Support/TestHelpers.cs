using System.Collections.Concurrent;
using System.Text.Json;
using Azure.Messaging.ServiceBus;
using BookLending.Messaging.Application.Abstractions;
using BookLending.Messaging.Application.Messaging;
using BookLending.Messaging.Domain.Events;

namespace BookLending.Messaging.Tests.Support;

public static class Eventually
{
    public static async Task<bool> TrueAsync(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (condition()) return true;
            await Task.Delay(100);
        }
        return condition();
    }

    public static async Task<T> ValueAsync<T>(Func<Task<T>> get, Func<T, bool> ok, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        T value = await get();
        while (!ok(value) && DateTime.UtcNow < deadline)
        {
            await Task.Delay(250);
            value = await get();
        }
        return value;
    }
}

public static class Sample
{
    public static BookBorrowed Borrowed() => new()
    {
        LoanId = Guid.NewGuid(),
        BookId = Guid.NewGuid(),
        MemberId = Guid.NewGuid(),
        DueDate = new DateOnly(2026, 12, 1),
    };

    public static BookReturned Returned(string category = "Fiction", int daysOverdue = 0) => new()
    {
        LoanId = Guid.NewGuid(),
        BookId = Guid.NewGuid(),
        MemberId = Guid.NewGuid(),
        Category = category,
        DaysOverdue = daysOverdue,
    };

    /// <summary>
    /// Builds a message exactly the way Exercise 1 says it should look, so consumer tests
    /// (Ex 2, 3) don't depend on your publisher.
    /// </summary>
    public static ServiceBusMessage AsMessage(LoanEvent evt) =>
        new(JsonSerializer.SerializeToUtf8Bytes(evt, evt.GetType(), MessagingJson.Options))
        {
            ContentType = "application/json",
            Subject = LoanEventTypes.NameOf(evt),
            MessageId = evt.EventId.ToString(),
            CorrelationId = evt.LoanId.ToString(),
        };

    public static T Deserialize<T>(ServiceBusReceivedMessage msg) =>
        JsonSerializer.Deserialize<T>(msg.Body.ToArray(), MessagingJson.Options)
        ?? throw new InvalidOperationException("Body deserialized to null");
}

public sealed class RecordingLoanEventHandler : ILoanEventHandler
{
    private int _calls;
    public ConcurrentQueue<LoanEvent> Handled { get; } = new();
    public int Calls => Volatile.Read(ref _calls);

    /// <summary>Optional: make the handler throw for specific events.</summary>
    public Func<LoanEvent, Task>? Behaviour { get; set; }

    public async Task HandleAsync(LoanEvent evt, CancellationToken ct)
    {
        Interlocked.Increment(ref _calls);
        if (Behaviour is not null) await Behaviour(evt);
        Handled.Enqueue(evt);
    }
}

public sealed class RecordingMemberActivityHandler : IMemberActivityHandler
{
    private int _inFlight;
    public ConcurrentQueue<MemberActivity> Handled { get; } = new();
    public int MaxObservedConcurrency { get; private set; }

    public async Task HandleAsync(MemberActivity activity, CancellationToken ct)
    {
        var now = Interlocked.Increment(ref _inFlight);
        lock (this) { MaxObservedConcurrency = Math.Max(MaxObservedConcurrency, now); }
        // Small random delay: without sessions, this is what scrambles the order.
        await Task.Delay(Random.Shared.Next(5, 40), ct);
        Handled.Enqueue(activity);
        Interlocked.Decrement(ref _inFlight);
    }
}
