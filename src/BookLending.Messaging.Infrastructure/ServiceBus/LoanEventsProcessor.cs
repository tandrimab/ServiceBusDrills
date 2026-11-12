using Azure.Messaging.ServiceBus;
using BookLending.Messaging.Application.Abstractions;
using BookLending.Messaging.Application.Messaging;
using BookLending.Messaging.Domain.Events;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Text.Json;

namespace BookLending.Messaging.Infrastructure.ServiceBus;

/// <summary>Background consumer for the "loan-events" queue.</summary>
public sealed class LoanEventsProcessor : IHostedService, IAsyncDisposable
{
    private readonly ServiceBusClient _client;
    private readonly ILoanEventHandler _handler;
    private readonly ILogger<LoanEventsProcessor> _logger;
    private ServiceBusProcessor? _processor;

    public LoanEventsProcessor(ServiceBusClient client, ILoanEventHandler handler, ILogger<LoanEventsProcessor> logger)
    {
        _client = client;
        _handler = handler;
        _logger = logger;
    }

    // =====================================================================
    // EXERCISE 2 — Receive, handle, complete
    // =====================================================================
    // StartAsync: create a processor for EntityNames.LoanEventsQueue and start it.
    //   * YOU decide when a message is settled: turn automatic completion OFF.
    //   * Process one message at a time (keeps Ex 3's test deterministic).
    //   * A processor needs BOTH a message handler and an error handler before it starts.
    //
    // For each message:
    //   1. Find the CLR type from the Subject (LoanEventTypes.Resolve).
    //   2. Deserialize the body into that type (MessagingJson.Options).
    //   3. Call _handler.HandleAsync(evt, ...).
    //   4. Only after the handler succeeds, complete the message.
    //
    // StopAsync: stop processing. After StopAsync returns, new messages must stay in the queue.
    //
    // =====================================================================
    // EXERCISE 3 — When things go wrong (extend the same message handler)
    // =====================================================================
    //   * Subject has no known type         -> dead-letter, reason DeadLetterReasons.UnknownEventType
    //   * Body isn't valid JSON for the type -> dead-letter, reason DeadLetterReasons.DeserializationFailed
    //   * Handler throws PoisonMessageException
    //                                       -> dead-letter, reason DeadLetterReasons.PoisonMessage,
    //                                          description = the exception's Message. Handler called ONCE.
    //   * Handler throws anything else (transient)
    //                                       -> abandon, so Service Bus redelivers it. After MaxDeliveryCount (3)
    //                                          attempts Service Bus dead-letters it by itself.
    //   * The error handler: log it. Don't throw from it.
    //
    // Then implement ServiceBusDeadLetterInspector.
    // =====================================================================

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        ServiceBusProcessorOptions options = new ServiceBusProcessorOptions();
        options.AutoCompleteMessages = false;
        options.MaxConcurrentCalls = 1;
        _processor = _client.CreateProcessor(EntityNames.LoanEventsQueue, options);
        Func<ProcessMessageEventArgs, Task> messageHandler = new Func<ProcessMessageEventArgs, Task>(MesssageHandler);
        Func<ProcessErrorEventArgs, Task> errorHandler = new Func<ProcessErrorEventArgs, Task>(ErrorHandler);
        _processor.ProcessMessageAsync += messageHandler;
        _processor.ProcessErrorAsync += errorHandler;
        await _processor.StartProcessingAsync(cancellationToken);
    }

    private async Task MesssageHandler(ProcessMessageEventArgs processMessageEventArgs)
    {
        ServiceBusReceivedMessage message = processMessageEventArgs.Message;
        CancellationToken ct = processMessageEventArgs.CancellationToken;
        var type = LoanEventTypes.Resolve(message.Subject);
        if (type is null)
        {
            await DeadLetterHandler(processMessageEventArgs, DeadLetterReasons.UnknownEventType,
                $"No event type registered for subject '{message.Subject}'", ct);
            return;
        }
        LoanEvent? body;
        try
        {
            body = (LoanEvent?)JsonSerializer.Deserialize(message.Body.ToMemory().Span, type, MessagingJson.Options);
        }
        catch (JsonException ex)
        {
            body = null;
            _logger.LogWarning(ex, "Could not deserialize message {MessageId}", message.MessageId);
        }
        if (body is null)
        {
            await DeadLetterHandler(processMessageEventArgs, DeadLetterReasons.DeserializationFailed,
                $"Body is not valid JSON for {type.Name}", ct);
            return;
        }
        try
        {
            await _handler.HandleAsync(body, ct);
        }
        catch (PoisonMessageException ex)
        {
            _logger.LogError(ex, "Poison message {MessageId}", message.MessageId);
            await DeadLetterHandler(processMessageEventArgs, DeadLetterReasons.PoisonMessage, ex.Message, ct);
            return;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Transient failure on message {MessageId} (attempt {DeliveryCount})",
                message.MessageId, message.DeliveryCount);
            await processMessageEventArgs.AbandonMessageAsync(message, cancellationToken: ct);
            return;
        }
        await processMessageEventArgs.CompleteMessageAsync(message, ct);
    }

    private static Task DeadLetterHandler(ProcessMessageEventArgs args, string reason, string description, CancellationToken ct) =>
        args.DeadLetterMessageAsync(args.Message, reason, description, ct);

    private Task ErrorHandler(ProcessErrorEventArgs args)
    {
        _logger.LogError(args.Exception, "Service Bus error from {ErrorSource} on {EntityPath}",
            args.ErrorSource, args.EntityPath);
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_processor is null) return;
        await _processor.StopProcessingAsync(cancellationToken);
        await _processor.DisposeAsync();
    }

    public async ValueTask DisposeAsync()
    {
        if (_processor is not null)
        {
            await _processor.DisposeAsync();
        }
    }
}
