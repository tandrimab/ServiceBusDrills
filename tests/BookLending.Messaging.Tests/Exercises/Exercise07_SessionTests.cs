using BookLending.Messaging.Application.Messaging;
using BookLending.Messaging.Domain.Events;
using BookLending.Messaging.Infrastructure.ServiceBus;
using BookLending.Messaging.Tests.Support;
using Microsoft.Extensions.Logging.Abstractions;

namespace BookLending.Messaging.Tests.Exercises;

[Collection(ServiceBusCollection.Name)]
public sealed class Exercise07_SessionTests(ServiceBusFixture fx) : IAsyncLifetime
{
    private static readonly string[] Members = ["m-ana", "m-bo", "m-cy"];
    private const int PerMember = 20;

    public Task InitializeAsync() => fx.PurgeSessionQueueAsync(EntityNames.MemberActivityQueue);
    public Task DisposeAsync() => Task.CompletedTask;

    [Fact]
    public async Task Each_members_activities_are_handled_in_order_and_members_run_in_parallel()
    {
        await using var publisher = new ServiceBusMemberActivityPublisher(fx.Client);
        var handler = new RecordingMemberActivityHandler();
        await using var processor = new MemberActivityProcessor(fx.Client, handler, NullLogger<MemberActivityProcessor>.Instance);

        // Interleaved: ana#1, bo#1, cy#1, ana#2, bo#2, ...
        for (var seq = 1; seq <= PerMember; seq++)
        {
            foreach (var member in Members)
            {
                await publisher.PublishAsync(new MemberActivity(member, seq, $"action-{seq}"));
            }
        }

        await processor.StartAsync(CancellationToken.None);
        var total = Members.Length * PerMember;
        var done = await Eventually.TrueAsync(() => handler.Handled.Count >= total, TimeSpan.FromSeconds(60));
        await processor.StopAsync(CancellationToken.None);

        Assert.True(done, $"Expected {total} handled activities, got {handler.Handled.Count}.");
        var handled = handler.Handled.ToList();
        Assert.Equal(total, handled.Count);

        foreach (var member in Members)
        {
            var order = handled.Where(a => a.MemberId == member).Select(a => a.Sequence).ToList();
            Assert.Equal(Enumerable.Range(1, PerMember), order);
        }

        Assert.True(handler.MaxObservedConcurrency >= 2,
            "Members should be processed in parallel (more than one session at a time).");
    }
}
