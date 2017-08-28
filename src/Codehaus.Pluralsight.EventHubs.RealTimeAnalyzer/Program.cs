using System;
using Microsoft.ServiceBus.Messaging;

namespace Codehaus.Pluralsight.EventHubs.RealTimeAnalyzer
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            const string eventHubConnectionString =
                "Endpoint=sb://mooney.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SATW/1oIIF9dAAPbgF3YLkDjMmGsYMvs/Fi+Vutz8bo=";
            const string eventHubName = "myeventhub";
            const string storageAccountName = "mooneyeventhub";
            const string storageAccountKey =
                "f++Lw/zix9ryWhgRRMqyRqr4QP4Np17BT21IbEzyEGH3SK7gT1byIdUPW7iVHau9xF8fTtANtC5c4ky+RUKJCw==";
            var storageConnectionString =
                $"DefaultEndpointsProtocol=https;AccountName={storageAccountName};AccountKey={storageAccountKey}";

            var eventProcessorHostName = Guid.NewGuid().ToString();
            var eventProcessorHost = new EventProcessorHost(
                eventProcessorHostName,
                eventHubName,
                "realtime",
                eventHubConnectionString,
                storageConnectionString);
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine("Registering with Event Hubs...");
            var options = new EventProcessorOptions();
            options.ExceptionReceived += (sender, e) => { Console.WriteLine(e.Exception); };
            eventProcessorHost.RegisterEventProcessorAsync<RealTimeConsumer>(options).Wait();

            Console.WriteLine("Receiving. Press enter key to stop worker...");
            Console.ReadLine();
            eventProcessorHost.UnregisterEventProcessorAsync().Wait();
        }
    }
}