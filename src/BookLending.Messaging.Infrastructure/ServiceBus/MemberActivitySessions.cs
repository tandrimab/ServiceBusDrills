using Azure.Messaging.ServiceBus;
using BookLending.Messaging.Application.Abstractions;
using BookLending.Messaging.Application.Messaging;
using BookLending.Messaging.Domain.Events;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace BookLending.Messaging.Infrastructure.ServiceBus;

// =========================================================================
// EXERCISE 7 — Sessions (ordered processing per member)
// =========================================================================
// "member-activity" has RequiresSession = true. Requirement: activities for the SAME member are
// handled strictly in the order they were sent, while DIFFERENT members are handled in parallel.
//
// Publisher:
//   * JSON body of the MemberActivity, ContentType "application/json".
//   * Group messages by member. (Hint: a session queue rejects messages without that grouping key.)
//
// Processor:
//   * Use the session-aware processor, not the normal one.
//   * Up to 4 members at once, but only 1 message at a time within a member.
//   * Manual completion, like Ex 2. On handler failure, abandon.
//   * Same Start/Stop behaviour as Ex 2.
//
// The test sends 20 interleaved activities each for 3 members and checks each member's sequence
// numbers were handled in ascending order.
// =========================================================================

public sealed class ServiceBusMemberActivityPublisher : IMemberActivityPublisher, IAsyncDisposable
{
    private readonly ServiceBusClient _client;

    public ServiceBusMemberActivityPublisher(ServiceBusClient client)
    {
        _client = client;
    }

    public Task PublishAsync(MemberActivity activity, CancellationToken ct = default)
    {
        throw new NotImplementedException("Exercise 7");
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

public sealed class MemberActivityProcessor : IHostedService, IAsyncDisposable
{
    private readonly ServiceBusClient _client;
    private readonly IMemberActivityHandler _handler;
    private readonly ILogger<MemberActivityProcessor> _logger;

    public MemberActivityProcessor(ServiceBusClient client, IMemberActivityHandler handler, ILogger<MemberActivityProcessor> logger)
    {
        _client = client;
        _handler = handler;
        _logger = logger;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        throw new NotImplementedException("Exercise 7");
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        throw new NotImplementedException("Exercise 7");
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
