using Azure.Messaging.ServiceBus;
using BookLending.Messaging.Application.Abstractions;
using BookLending.Messaging.Application.Messaging;
using BookLending.Messaging.Domain.Events;
using Microsoft.Azure.Amqp.Framing;

namespace BookLending.Messaging.Infrastructure.ServiceBus;

public sealed class ServiceBusBookEventBroadcaster : IBookEventBroadcaster, IAsyncDisposable
{
    private ServiceBusSender _sender;

    public ServiceBusBookEventBroadcaster(ServiceBusClient client)
    {
        _sender = client.CreateSender(EntityNames.BookEventsTopic);
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
    public async Task BroadcastReturnedAsync(BookReturned evt, CancellationToken ct = default)
    {
        ServiceBusMessage message = new ServiceBusMessage(BinaryData.FromObjectAsJson(evt, MessagingJson.Options))
        {
            Subject = evt.GetType().ToString(),
            ContentType = "application/json",
            MessageId = evt.EventId.ToString(),
            CorrelationId = evt.LoanId.ToString()
        };
        message.ApplicationProperties["category"] = evt.Category;
        message.ApplicationProperties["daysOverdue"] = evt.DaysOverdue;
        await _sender.SendMessageAsync(message, ct);

    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

// Your answer:
// The same message needs to be created for each consumer, which means 3 queues. Instead, in case of topic a copy of the message will be published to all consumers who subscribe for that and the filters will be applied.
