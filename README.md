# Service Bus Drills (BookLending edition)

Eight exercises that build up Azure Service Bus skills step by step. Everything is already set up:
the domain, the application ports, DI, an API, a local Service Bus emulator and **tests that check
each exercise**. You only write the Service Bus code in:

```
src/BookLending.Messaging.Infrastructure/ServiceBus/
```

Every TODO file says exactly what the tests expect. Don't change the tests, `Application/` or `Domain/`.

## Layout

```
src/
  BookLending.Messaging.Domain/          events (done)
  BookLending.Messaging.Application/     ports/interfaces, entity names, JSON options (done)
  BookLending.Messaging.Infrastructure/  ServiceBus/*.cs  ← YOUR CODE
  BookLending.Messaging.Api/             minimal API that uses your code (done)
tests/
  BookLending.Messaging.Tests/           one test class per exercise
emulator/Config.json                     queues, topic, subscriptions, filters
docker-compose.yml, .env                 local emulator
```

## Setup (once)

1. You need the .NET 10 SDK and Docker running.
2. Read the emulator EULA, then set `ACCEPT_EULA=Y` in `.env`.
3. Start the emulator:
   ```bash
   docker compose up -d
   curl http://localhost:5300/health     # wait until it answers healthy (about 30–60 s)
   ```
4. Check the build: `dotnet build`

The emulator forgets all messages when the container restarts. If you change `emulator/Config.json`,
restart it with `docker compose down && docker compose up -d`.

## The exercises

Do them in order. Run just the one you're working on:

```bash
dotnet test --filter "FullyQualifiedName~Exercise01"
```

| # | Concept | File | What you build |
|---|---------|------|----------------|
| 1 | Sending | `ServiceBusLoanEventPublisher.cs` → `PublishAsync` | One JSON message with the right metadata (Subject, MessageId, CorrelationId, ContentType) |
| 2 | Receiving | `LoanEventsProcessor.cs` | A processor running as a hosted service; deserialize by Subject; complete by hand |
| 3 | Failure handling | `LoanEventsProcessor.cs` + `ServiceBusDeadLetterInspector.cs` | Dead-letter vs abandon, MaxDeliveryCount, peeking at the DLQ |
| 4 | Pub/sub | `ServiceBusBookEventBroadcaster.cs` | Publish to a topic; make SQL and correlation filters route correctly |
| 5 | Throughput | `ServiceBusLoanEventPublisher.cs` → `PublishBatchAsync` | Split a big list across size-limited batches |
| 6 | Delayed delivery | `ServiceBusReminderScheduler.cs` | Schedule and cancel messages |
| 7 | Ordering | `MemberActivitySessions.cs` | Sessions: in order per member, parallel across members |
| 8 | Idempotent sends | `ServiceBusFinePublisher.cs` | Duplicate detection through a deterministic MessageId |

Exercises 4 and 8 also ask a short design question. Answer it in a comment in the file.

Package docs you'll need: `Azure.Messaging.ServiceBus`, especially `ServiceBusSender`, `ServiceBusProcessor`,
`ServiceBusSessionProcessor`, `ServiceBusReceiver` and `ServiceBusMessageBatch`.

## Getting it checked

When an exercise (or all of them) passes:

1. Push to GitHub and send me the repo link, **or** paste the files you changed.
2. Include the output of `dotnet test` (or `dotnet test --filter ...` for one exercise).

Passing tests are only part of it. I'll also review what tests can't catch:
- client/sender lifetime and disposal (who owns what)
- auto-complete really turned off, and messages settled exactly once
- cancellation tokens passed through
- what happens if `CompleteMessageAsync` fails after the handler already succeeded
- logging in the error handler, and nothing thrown from it
- your answers to the design questions

## If you're stuck

Ask for a **hint** for exercise N. I'll nudge, not give away the code.
