namespace BookLending.Messaging.Domain.Events;

/// <summary>Base type for everything that happens to a loan.</summary>
public abstract record LoanEvent
{
    public Guid EventId { get; init; } = Guid.NewGuid();
    public Guid LoanId { get; init; }
    public DateTimeOffset OccurredAt { get; init; } = DateTimeOffset.UtcNow;
}

public sealed record BookBorrowed : LoanEvent
{
    public Guid BookId { get; init; }
    public Guid MemberId { get; init; }
    public DateOnly DueDate { get; init; }
}

public sealed record BookReturned : LoanEvent
{
    public Guid BookId { get; init; }
    public Guid MemberId { get; init; }
    public string Category { get; init; } = "General";
    public int DaysOverdue { get; init; }
}
