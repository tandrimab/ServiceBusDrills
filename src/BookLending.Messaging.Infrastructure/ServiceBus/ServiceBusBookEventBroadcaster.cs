using Azure.Messaging.ServiceBus;
using BookLending.Messaging.Application.Abstractions;
using BookLending.Messaging.Application.Messaging;
using BookLending.Messaging.Domain.Events;

namespace BookLending.Messaging.Infrastructure.ServiceBus;

public sealed class ServiceBusBookEventBroadcaster : IBookEventBroadcaster, IAsyncDisposable
{
    private readonly ServiceBusClient _client;

    public ServiceBusBookEventBroadcaster(ServiceBusClient client)
    {
        _client = client;
    }

    // =====================================================================
    // EXERCISE 4 — Topics, subscriptions and filters
    // =====================================================================
    // Publish `evt` to the "book-events" TOPIC (not a queue). Three subscriptions already exist
    // (see emulator/Config.json):
    //   * email-notifications  -> no filter: gets everything
    //   * overdue-alerts       -> SQL filter:          daysOverdue > 0
    //   * fiction-analytics    -> correlation filter:  category = 'Fiction'
    //
    // Filters can't read the JSON body. Make the routing work by setting what they CAN see:
    //   * application property "category"    = evt.Category      (string)
    //   * application property "daysOverdue" = evt.DaysOverdue   (int, not a string)
    // Plus the same basics as Ex 1: JSON body, ContentType "application/json",
    // Subject "BookReturned", MessageId = EventId.
    //
    // Then answer in a comment at the bottom of this file: why is a topic better here than
    // three separate queues the publisher sends to?
    // =====================================================================
    public Task BroadcastReturnedAsync(BookReturned evt, CancellationToken ct = default)
    {
        throw new NotImplementedException("Exercise 4");
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

// Your answer:
//
