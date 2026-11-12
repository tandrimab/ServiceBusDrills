using Azure.Messaging.ServiceBus;
using BookLending.Messaging.Application.Abstractions;
using BookLending.Messaging.Application.Messaging;
using BookLending.Messaging.Domain.Events;
using Microsoft.Azure.Amqp;

namespace BookLending.Messaging.Infrastructure.ServiceBus;

public sealed class ServiceBusReminderScheduler : IReminderScheduler, IAsyncDisposable
{
    private ServiceBusSender _sender;

    public ServiceBusReminderScheduler(ServiceBusClient client)
    {
        _sender = client.CreateSender(EntityNames.DueRemindersQueue);

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
    public async Task<long> ScheduleAsync(DueDateReminder reminder, DateTimeOffset deliverAt, CancellationToken ct = default)
    {
        ServiceBusMessage message = new ServiceBusMessage(BinaryData.FromObjectAsJson(reminder, MessagingJson.Options))
        {
            Subject = reminder.GetType().ToString(),
            ContentType = "application/json",
            MessageId = reminder.MemberId.ToString(),
            CorrelationId = reminder.LoanId.ToString()
        };
        return await _sender.ScheduleMessageAsync(message, deliverAt, ct);
    }

    public async Task CancelAsync(long sequenceNumber, CancellationToken ct = default)
    {
        await _sender.CancelScheduledMessageAsync(sequenceNumber, ct);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
