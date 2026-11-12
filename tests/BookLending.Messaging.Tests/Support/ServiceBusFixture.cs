using Azure.Messaging.ServiceBus;

namespace BookLending.Messaging.Tests.Support;

/// <summary>
/// One shared ServiceBusClient for the whole test run (the emulator allows only a few connections),
/// plus helpers to empty entities so every test starts clean.
/// </summary>
public sealed class ServiceBusFixture : IAsyncLifetime
{
    public const string EmulatorConnectionString =
        "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;";

    public ServiceBusClient Client { get; private set; } = null!;

    public Task InitializeAsync()
    {
        // Set SERVICEBUS_CONNECTION to run against a real namespace instead of the emulator.
        var cs = Environment.GetEnvironmentVariable("SERVICEBUS_CONNECTION") ?? EmulatorConnectionString;
        Client = new ServiceBusClient(cs);
        return Task.CompletedTask;
    }

    public async Task DisposeAsync() => await Client.DisposeAsync();

    public Task PurgeQueueAsync(string queue) =>
        DrainAsync(Client.CreateReceiver(queue, ReceiveAndDelete()));

    public Task PurgeDeadLetterAsync(string queue) =>
        DrainAsync(Client.CreateReceiver(queue, ReceiveAndDelete(SubQueue.DeadLetter)));

    public Task PurgeSubscriptionAsync(string topic, string subscription) =>
        DrainAsync(Client.CreateReceiver(topic, subscription, ReceiveAndDelete()));

    public async Task PurgeSessionQueueAsync(string queue)
    {
        while (true)
        {
            ServiceBusSessionReceiver receiver;
            try
            {
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
                receiver = await Client.AcceptNextSessionAsync(queue,
                    new ServiceBusSessionReceiverOptions { ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete }, cts.Token);
            }
            catch (OperationCanceledException) { return; }
            catch (ServiceBusException ex) when (ex.Reason == ServiceBusFailureReason.ServiceTimeout) { return; }

            await DrainAsync(receiver);
        }
    }

    /// <summary>Receives (and deletes) everything currently available.</summary>
    public async Task<List<ServiceBusReceivedMessage>> ReceiveAllAsync(ServiceBusReceiver receiver, int expected, TimeSpan timeout)
    {
        var all = new List<ServiceBusReceivedMessage>();
        var deadline = DateTime.UtcNow + timeout;
        while (all.Count < expected && DateTime.UtcNow < deadline)
        {
            var batch = await receiver.ReceiveMessagesAsync(Math.Min(250, expected - all.Count), TimeSpan.FromSeconds(1));
            all.AddRange(batch);
        }
        // Grab any extras, so tests can assert "exactly N".
        all.AddRange(await receiver.ReceiveMessagesAsync(250, TimeSpan.FromSeconds(1)));
        return all;
    }

    private static ServiceBusReceiverOptions ReceiveAndDelete(SubQueue subQueue = SubQueue.None) =>
        new() { ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete, SubQueue = subQueue };

    private static async Task DrainAsync(ServiceBusReceiver receiver)
    {
        await using (receiver)
        {
            while ((await receiver.ReceiveMessagesAsync(250, TimeSpan.FromMilliseconds(750))).Count > 0) { }
        }
    }
}

[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class ServiceBusCollection : ICollectionFixture<ServiceBusFixture>
{
    public const string Name = "ServiceBus";
}
