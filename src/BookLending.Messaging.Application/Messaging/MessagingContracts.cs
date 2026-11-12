using System.Text.Json;
using BookLending.Messaging.Domain.Events;

namespace BookLending.Messaging.Application.Messaging;

/// <summary>JSON settings every producer and consumer must share.</summary>
public static class MessagingJson
{
    public static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web);
}

/// <summary>
/// Maps the message Subject (the event "name") to its CLR type and back.
/// Producers put the name in <c>Subject</c>; consumers use it to pick the type to deserialize.
/// </summary>
public static class LoanEventTypes
{
    private static readonly Dictionary<string, Type> ByName = new()
    {
        [nameof(BookBorrowed)] = typeof(BookBorrowed),
        [nameof(BookReturned)] = typeof(BookReturned),
    };

    public static string NameOf(LoanEvent evt) => evt.GetType().Name;

    public static Type? Resolve(string? subject) =>
        subject is not null && ByName.TryGetValue(subject, out var type) ? type : null;
}

/// <summary>Reason codes the tests look for in the dead-letter queue (Exercise 3).</summary>
public static class DeadLetterReasons
{
    public const string PoisonMessage = "PoisonMessage";
    public const string UnknownEventType = "UnknownEventType";
    public const string DeserializationFailed = "DeserializationFailed";
}

/// <summary>Queue / topic / subscription names. They match emulator/Config.json.</summary>
public static class EntityNames
{
    public const string LoanEventsQueue = "loan-events";
    public const string DueRemindersQueue = "due-reminders";
    public const string MemberActivityQueue = "member-activity";
    public const string FinesQueue = "fines";

    public const string BookEventsTopic = "book-events";
    public const string EmailNotificationsSubscription = "email-notifications";
    public const string OverdueAlertsSubscription = "overdue-alerts";
    public const string FictionAnalyticsSubscription = "fiction-analytics";
}
