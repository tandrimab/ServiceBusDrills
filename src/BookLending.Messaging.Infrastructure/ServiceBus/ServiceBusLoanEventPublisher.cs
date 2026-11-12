using Azure.Messaging.ServiceBus;
using BookLending.Messaging.Application.Abstractions;
using BookLending.Messaging.Application.Messaging;
using BookLending.Messaging.Domain.Events;

namespace BookLending.Messaging.Infrastructure.ServiceBus;

public sealed class ServiceBusLoanEventPublisher : ILoanEventPublisher, IAsyncDisposable
{
    private readonly ServiceBusClient _client;

    private readonly ServiceBusSender _sender;

    public ServiceBusLoanEventPublisher(ServiceBusClient client)
    {
        _client = client;
        // TODO (Ex 1): you'll probably want a sender for EntityNames.LoanEventsQueue, created once and reused.
        _sender = _client.CreateSender(EntityNames.LoanEventsQueue);
    }

    // =====================================================================
    // EXERCISE 1 — Send one message
    // =====================================================================
    // Send `evt` to the "loan-events" queue. The test checks the message has:
    //   * Body          = the event as JSON (use MessagingJson.Options), deserializable back to the
    //                     *concrete* type with all its fields (DueDate, MemberId, …)
    //   * ContentType   = "application/json"
    //   * Subject       = LoanEventTypes.NameOf(evt)        e.g. "BookBorrowed"
    //   * MessageId     = evt.EventId.ToString()
    //   * CorrelationId = evt.LoanId.ToString()
    //
    // Watch out: serializing a `LoanEvent`-typed variable only writes the base-class properties.
    // =====================================================================
    public Task PublishAsync<TEvent>(TEvent evt, CancellationToken ct = default) where TEvent : LoanEvent
    {
        var message = new ServiceBusMessage(BinaryData.FromObjectAsJson(evt, MessagingJson.Options))
        {
            Subject = LoanEventTypes.NameOf(evt),
            ContentType = "application/json",
            MessageId = evt.EventId.ToString(),
            CorrelationId = evt.LoanId.ToString()
        };
        return _sender.SendMessageAsync(message, ct);
    }

    // =====================================================================
    // EXERCISE 5 — Batching
    // =====================================================================
    // Send every event in `events` (same message shape as Exercise 1) using as few round-trips
    // as possible, and return how many batches you sent.
    //   * Empty list -> send nothing, return 0.
    //   * A batch has a size limit. When a message doesn't fit, send the current batch and start a
    //     new one. Don't lose the message that didn't fit.
    //   * If a single message can't fit even in an empty batch, throw.
    //   * Batches are IDisposable.
    // The test sends 3,000 events (more than one batch) and checks all 3,000 arrive exactly once.
    // =====================================================================
    public Task<int> PublishBatchAsync<TEvent>(IReadOnlyList<TEvent> events, CancellationToken ct = default) where TEvent : LoanEvent
    {
        throw new NotImplementedException("Exercise 5");
    }

    public ValueTask DisposeAsync()
    {
        // TODO: dispose whatever you created in the constructor (but NOT the injected client — you don't own it).
        return ValueTask.CompletedTask;
    }
}
