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

    public Task StartAsync(CancellationToken cancellationToken)
    {
        ServiceBusProcessorOptions options = new ServiceBusProcessorOptions();
        options.AutoCompleteMessages = false;
        options.MaxConcurrentCalls = 1;
        _processor = _client.CreateProcessor(EntityNames.LoanEventsQueue, options);
        Func<ProcessMessageEventArgs, Task> messageHandler = new Func<ProcessMessageEventArgs, Task>(MesssageHandler);
        Func<ProcessErrorEventArgs, Task> errorHandler = new Func<ProcessErrorEventArgs, Task>(ErrorHandler);
        return _processor.StartProcessingAsync(cancellationToken);
    }

    private async Task MesssageHandler(ProcessMessageEventArgs processMessageEventArgs)
    {
        ServiceBusReceivedMessage message = processMessageEventArgs.Message;
        CancellationToken ct = processMessageEventArgs.CancellationToken;
        var type = LoanEventTypes.Resolve(message.Subject);
        var body = (LoanEvent?)JsonSerializer.Deserialize(message.Body.ToMemory().Span,
        type,
        MessagingJson.Options
        );

        await _handler.HandleAsync(body, ct);
        await processMessageEventArgs.CompleteMessageAsync(message, ct);
    }

    private Task ErrorHandler(ProcessErrorEventArgs processErrorEventArgs)
    {
        string text = string.Format($"{0}: {1}", processErrorEventArgs.ErrorSource, processErrorEventArgs.Exception.Message);
        Console.WriteLine(text);
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _processor.StopProcessingAsync(cancellationToken);
        return Task.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        if (_processor is not null)
        {
            await _processor.DisposeAsync();
        }
    }
}
