using System;

namespace Codehaus.Pluralsight.EventHubs.PublisherApp
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            var publisher = new Publisher();

            publisher.Init(
                "Endpoint=sb://mooney.servicebus.windows.net/;SharedAccessKeyName=Publish;SharedAccessKey=YF3Eu+QUmfuxBXhlWvibaKRPVfgKvGr9eo25FId3nIk=;EntityPath=myeventhub");

            var random = new Random(Environment.TickCount);
            const int numEvents = 1000;

            Console.ForegroundColor = ConsoleColor.Green;

            for (var i = 0; i < numEvents; i++)
            {
                var deviceTelemetry = DeviceTelemetry.GenerateRandom(random);
                publisher.Publish(deviceTelemetry);
                Console.WriteLine($"Published {i + 1} events...");
            }
            Console.ReadLine();
        }
    }
}