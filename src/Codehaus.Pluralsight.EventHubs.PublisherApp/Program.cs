using System;

namespace Codehaus.Pluralsight.EventHubs.PublisherApp
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            var publisher = new Publisher();

            publisher.Init(
                "Endpoint=sb://simple-example.servicebus.windows.net/;SharedAccessKeyName=Publisher;SharedAccessKey=WFF0D3kZAme2nTeG7Nun2oc/YQIkX280pwbULWAF1OM=;EntityPath=simple-example");

            var random = new Random(Environment.TickCount);
            const int numEvents = 1000;

            Console.ForegroundColor = ConsoleColor.Green;

            for (var i = 0; i < numEvents; i++)
            {
                var deviceTelemetry = DeviceTelemetry.GenerateRandom(random);
                publisher.Publish(deviceTelemetry);
                Console.Clear();
                Console.WriteLine($"Published {i + 1} events...");
            }
            Console.ReadLine();
        }
    }
}