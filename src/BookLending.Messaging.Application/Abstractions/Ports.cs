using BookLending.Messaging.Domain.Events;

namespace BookLending.Messaging.Application.Abstractions;

// ---------------------------------------------------------------------------
// Outgoing ports. The Application layer only knows these interfaces; the
// Service Bus implementations live in Infrastructure (that's your job).
// ---------------------------------------------------------------------------

/// <summary>Exercises 1 and 5. Sends loan events to the "loan-events" queue.</summary>
public interface ILoanEventPublisher
{
    Task PublishAsync<TEvent>(TEvent evt, CancellationToken ct = default) where TEvent : LoanEvent;

    /// <summary>Sends many events using as few batches as possible.</summary>
    /// <returns>How many batches were actually sent (0 for an empty list).</returns>
    Task<int> PublishBatchAsync<TEvent>(IReadOnlyList<TEvent> events, CancellationToken ct = default) where TEvent : LoanEvent;
}

/// <summary>Exercise 3. Reads (without removing) what ended up in the loan-events dead-letter queue.</summary>
public interface IDeadLetterInspector
{
    Task<IReadOnlyList<DeadLetteredMessage>> PeekAsync(int maxMessages = 50, CancellationToken ct = default);
}

public sealed record DeadLetteredMessage(
    string MessageId,
    string? Subject,
    string? Reason,
    string? Description,
    int DeliveryCount);

/// <summary>Exercise 4. Fan-out of BookReturned to every interested subscriber via the "book-events" topic.</summary>
public interface IBookEventBroadcaster
{
    Task BroadcastReturnedAsync(BookReturned evt, CancellationToken ct = default);
}

/// <summary>Exercise 6. Delayed delivery on the "due-reminders" queue.</summary>
public interface IReminderScheduler
{
    /// <returns>The sequence number Service Bus assigned to the scheduled message.</returns>
    Task<long> ScheduleAsync(DueDateReminder reminder, DateTimeOffset deliverAt, CancellationToken ct = default);

    Task CancelAsync(long sequenceNumber, CancellationToken ct = default);
}

/// <summary>Exercise 7. Ordered-per-member messages on the "member-activity" queue (sessions enabled).</summary>
public interface IMemberActivityPublisher
{
    Task PublishAsync(MemberActivity activity, CancellationToken ct = default);
}

/// <summary>Exercise 8. Fines on the "fines" queue (duplicate detection enabled).</summary>
public interface IFinePublisher
{
    Task PublishAsync(FineIssued fine, CancellationToken ct = default);
}

// ---------------------------------------------------------------------------
// Incoming ports: business logic the processors call. Already implemented
// elsewhere (the Api logs them; the tests record them).
// ---------------------------------------------------------------------------

public interface ILoanEventHandler
{
    Task HandleAsync(LoanEvent evt, CancellationToken ct);
}

public interface IMemberActivityHandler
{
    Task HandleAsync(MemberActivity activity, CancellationToken ct);
}

/// <summary>
/// Thrown by a handler when the message can never succeed, no matter how many retries
/// (bad data, broken business rule). Exercise 3: these go straight to the dead-letter queue.
/// Any other exception is treated as transient.
/// </summary>
public sealed class PoisonMessageException(string message) : Exception(message);
