using Azure.Messaging.ServiceBus;
using BookLending.Messaging.Application.Abstractions;
using BookLending.Messaging.Infrastructure.ServiceBus;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace BookLending.Messaging.Infrastructure;

// Already wired up. Read it to see the lifetimes, but you don't need to change it.
public static class DependencyInjection
{
    public static IServiceCollection AddServiceBusMessaging(this IServiceCollection services, IConfiguration config)
    {
        var connectionString = config.GetConnectionString("ServiceBus")
            ?? throw new InvalidOperationException("ConnectionStrings:ServiceBus is missing.");

        // ONE client for the whole app: it owns the AMQP connection. Senders/receivers are cheap and come from it.
        services.AddSingleton(_ => new ServiceBusClient(connectionString));

        services.AddSingleton<ILoanEventPublisher, ServiceBusLoanEventPublisher>();
        services.AddSingleton<IDeadLetterInspector, ServiceBusDeadLetterInspector>();
        services.AddSingleton<IBookEventBroadcaster, ServiceBusBookEventBroadcaster>();
        services.AddSingleton<IReminderScheduler, ServiceBusReminderScheduler>();
        services.AddSingleton<IMemberActivityPublisher, ServiceBusMemberActivityPublisher>();
        services.AddSingleton<IFinePublisher, ServiceBusFinePublisher>();

        services.AddHostedService<LoanEventsProcessor>();
        services.AddHostedService<MemberActivityProcessor>();

        return services;
    }
}
