using System;
using Microsoft.ServiceBus.Messaging;

namespace Codehaus.Pluralsight.EventHubs.RealTimeAnalyzer
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            const string eventHubConnectionString =
                "Endpoint=sb://simple-example.servicebus.windows.net/;SharedAccessKeyName=Consumer;SharedAccessKey=3ZzLmCvYRxl7WhL6wHqXCTqkdwMuvVmsmUQCZTUt4/w=";
            const string eventHubName = "simple-example";
            const string storageAccountName = "mooney";
            const string storageAccountKey =
                "Kdg1BzIfYPKdU+EWaqfkEIlmmq3uWbKU7t7TclkEVJEOvT8fe/N8LPa/xQyMYRiEong53BYfN6IpygRaASmFLQ==";
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