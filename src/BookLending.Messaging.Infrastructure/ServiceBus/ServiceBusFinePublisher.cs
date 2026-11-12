using Azure.Messaging.ServiceBus;
using BookLending.Messaging.Application.Abstractions;
using BookLending.Messaging.Application.Messaging;
using BookLending.Messaging.Domain.Events;

namespace BookLending.Messaging.Infrastructure.ServiceBus;

public sealed class ServiceBusFinePublisher : IFinePublisher, IAsyncDisposable
{
    private readonly ServiceBusClient _client;

    public ServiceBusFinePublisher(ServiceBusClient client)
    {
        _client = client;
    }

    // =====================================================================
    // EXERCISE 8 — Duplicate detection
    // =====================================================================
    // The API sometimes retries after a timeout, so the same fine can be published twice.
    // "fines" has RequiresDuplicateDetection = true (1-minute window).
    //
    // Publish the fine (JSON, ContentType "application/json") so that publishing the SAME fine
    // twice results in ONE message on the queue, but two DIFFERENT fines are both kept.
    // The test also expects MessageId = fine.FineId.ToString().
    //
    // Then answer in a comment below: duplicate detection only covers the sending side within a
    // time window. What else would you still need on the consuming side, and why?
    // =====================================================================
    public Task PublishAsync(FineIssued fine, CancellationToken ct = default)
    {
        throw new NotImplementedException("Exercise 8");
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

// Your answer:
//
