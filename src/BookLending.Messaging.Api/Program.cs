using BookLending.Messaging.Application.Abstractions;
using BookLending.Messaging.Domain.Events;
using BookLending.Messaging.Infrastructure;

// Already done. Once your exercises pass, run this and hit the endpoints to watch messages flow
// end to end (the handlers below just log).
var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton<ILoanEventHandler, LoggingLoanEventHandler>();
builder.Services.AddSingleton<IMemberActivityHandler, LoggingMemberActivityHandler>();
builder.Services.AddServiceBusMessaging(builder.Configuration);

var app = builder.Build();

app.MapPost("/loans/{loanId:guid}/borrow", async (Guid loanId, ILoanEventPublisher publisher, CancellationToken ct) =>
{
    var evt = new BookBorrowed
    {
        LoanId = loanId,
        BookId = Guid.NewGuid(),
        MemberId = Guid.NewGuid(),
        DueDate = DateOnly.FromDateTime(DateTime.UtcNow.AddDays(14)),
    };
    await publisher.PublishAsync(evt, ct);
    return Results.Accepted(value: evt);
});

app.MapPost("/loans/{loanId:guid}/return", async (Guid loanId, string category, int daysOverdue,
    ILoanEventPublisher publisher, IBookEventBroadcaster broadcaster, CancellationToken ct) =>
{
    var evt = new BookReturned
    {
        LoanId = loanId,
        BookId = Guid.NewGuid(),
        MemberId = Guid.NewGuid(),
        Category = category,
        DaysOverdue = daysOverdue,
    };
    await publisher.PublishAsync(evt, ct);
    await broadcaster.BroadcastReturnedAsync(evt, ct);
    return Results.Accepted(value: evt);
});

app.MapPost("/loans/{loanId:guid}/reminder", async (Guid loanId, int inSeconds, IReminderScheduler scheduler, CancellationToken ct) =>
{
    var reminder = new DueDateReminder(loanId, Guid.NewGuid(), "The Pragmatic Programmer",
        DateOnly.FromDateTime(DateTime.UtcNow.AddDays(1)));
    var seq = await scheduler.ScheduleAsync(reminder, DateTimeOffset.UtcNow.AddSeconds(inSeconds), ct);
    return Results.Ok(new { sequenceNumber = seq });
});

app.MapDelete("/reminders/{sequenceNumber:long}", async (long sequenceNumber, IReminderScheduler scheduler, CancellationToken ct) =>
{
    await scheduler.CancelAsync(sequenceNumber, ct);
    return Results.NoContent();
});

app.MapPost("/members/{memberId}/activity", async (string memberId, int sequence, string action,
    IMemberActivityPublisher publisher, CancellationToken ct) =>
{
    await publisher.PublishAsync(new MemberActivity(memberId, sequence, action), ct);
    return Results.Accepted();
});

app.MapPost("/fines", async (FineIssued fine, IFinePublisher publisher, CancellationToken ct) =>
{
    await publisher.PublishAsync(fine, ct);
    return Results.Accepted();
});

app.Run();

internal sealed class LoggingLoanEventHandler(ILogger<LoggingLoanEventHandler> logger) : ILoanEventHandler
{
    public Task HandleAsync(LoanEvent evt, CancellationToken ct)
    {
        logger.LogInformation("Handled {EventType}: {@Event}", evt.GetType().Name, evt);
        return Task.CompletedTask;
    }
}

internal sealed class LoggingMemberActivityHandler(ILogger<LoggingMemberActivityHandler> logger) : IMemberActivityHandler
{
    public Task HandleAsync(MemberActivity activity, CancellationToken ct)
    {
        logger.LogInformation("Member {MemberId} #{Sequence}: {Action}", activity.MemberId, activity.Sequence, activity.Action);
        return Task.CompletedTask;
    }
}
