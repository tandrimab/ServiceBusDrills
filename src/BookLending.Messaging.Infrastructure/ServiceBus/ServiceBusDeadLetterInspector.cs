using Azure.Messaging.ServiceBus;
using BookLending.Messaging.Application.Abstractions;
using BookLending.Messaging.Application.Messaging;

namespace BookLending.Messaging.Infrastructure.ServiceBus;

public sealed class ServiceBusDeadLetterInspector : IDeadLetterInspector
{
    private readonly ServiceBusClient _client;

    public ServiceBusDeadLetterInspector(ServiceBusClient client) => _client = client;

    // =====================================================================
    // EXERCISE 3 (part 2) — Look inside the dead-letter queue
    // =====================================================================
    // Return up to `maxMessages` messages from the DEAD-LETTER sub-queue of "loan-events",
    // WITHOUT removing or locking them (calling this twice must return the same messages).
    // Map each one to DeadLetteredMessage (MessageId, Subject, reason, error description, DeliveryCount).
    // =====================================================================
    public async Task<IReadOnlyList<DeadLetteredMessage>> PeekAsync(int maxMessages = 50, CancellationToken ct = default)
    {
        await using ServiceBusReceiver receiver = _client.CreateReceiver(
            EntityNames.LoanEventsQueue,
            new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });

        var result = new List<DeadLetteredMessage>();
        long? fromSequence = null;

        while (result.Count < maxMessages)
        {
            IReadOnlyList<ServiceBusReceivedMessage> batch = await receiver.PeekMessagesAsync(
                maxMessages - result.Count, fromSequence, ct);

            if (batch.Count == 0) break;

            foreach (var m in batch)
            {
                result.Add(new DeadLetteredMessage(
                    m.MessageId,
                    m.Subject,
                    m.DeadLetterReason,
                    m.DeadLetterErrorDescription,
                    m.DeliveryCount));
            }

            fromSequence = batch[^1].SequenceNumber + 1;
        }

        return result;
    }

}
