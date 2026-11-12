using Azure.Messaging.ServiceBus;
using BookLending.Messaging.Application.Abstractions;
using BookLending.Messaging.Application.Messaging;
using BookLending.Messaging.Domain.Events;

namespace BookLending.Messaging.Infrastructure.ServiceBus;

public sealed class ServiceBusReminderScheduler : IReminderScheduler, IAsyncDisposable
{
    private readonly ServiceBusClient _client;

    public ServiceBusReminderScheduler(ServiceBusClient client)
    {
        _client = client;
    }

    // =====================================================================
    // EXERCISE 6 — Scheduled messages
    // =====================================================================
    // ScheduleAsync: put the reminder (JSON) on "due-reminders" so it only becomes receivable
    // at `deliverAt`. Return the sequence number so it can be cancelled later.
    // CancelAsync: cancel a scheduled message by that sequence number.
    //
    // The test checks: nothing is receivable right away, the message shows up after deliverAt,
    // and a cancelled reminder never shows up.
    //
    // Think about: there are two ways to schedule. Which one gives you something to cancel with?
    // =====================================================================
    public Task<long> ScheduleAsync(DueDateReminder reminder, DateTimeOffset deliverAt, CancellationToken ct = default)
    {
        throw new NotImplementedException("Exercise 6");
    }

    public Task CancelAsync(long sequenceNumber, CancellationToken ct = default)
    {
        throw new NotImplementedException("Exercise 6");
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
