namespace BookLending.Messaging.Domain.Events;

/// <summary>A reminder that should reach the member shortly before the due date (Exercise 6).</summary>
public sealed record DueDateReminder(Guid LoanId, Guid MemberId, string BookTitle, DateOnly DueDate);

/// <summary>Something a member did. Must be processed in order per member (Exercise 7).</summary>
public sealed record MemberActivity(string MemberId, int Sequence, string Action);

/// <summary>A late fee. Must never be charged twice (Exercise 8).</summary>
public sealed record FineIssued(Guid FineId, Guid LoanId, decimal Amount);
