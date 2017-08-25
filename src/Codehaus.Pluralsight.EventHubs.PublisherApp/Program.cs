using System;

namespace Codehaus.Pluralsight.EventHubs.PublisherApp
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            var eventHubConnectionString =
                "Endpoint=sb://mooney.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SATW/1oIIF9dAAPbgF3YLkDjMmGsYMvs/Fi+Vutz8bo=";
            var eventHubName = "myeventhub";
            var storageAccountName = "mooneyeventhub";
            var storageAccountKey =
                "f++Lw/zix9ryWhgRRMqyRqr4QP4Np17BT21IbEzyEGH3SK7gT1byIdUPW7iVHau9xF8fTtANtC5c4ky+RUKJCw==";
            var storageConnectionString =
                $"DefaultEndpointsProtocol=https;AccountName={storageAccountName};AccountKey={storageAccountKey}";

            var publisher = new Publisher();

            publisher.Init(
                "Endpoint=sb://mooney.servicebus.windows.net/;SharedAccessKeyName=Publish;SharedAccessKey=YF3Eu+QUmfuxBXhlWvibaKRPVfgKvGr9eo25FId3nIk=;EntityPath=myeventhub");

            var random = new Random(Environment.TickCount);
            const int numEvents = 1000;

            for (var i = 0; i < numEvents; i++)
            {
                var deviceTelemetry = DeviceTelemetry.GenerateRandom(random);
                publisher.Publish(deviceTelemetry);
            }

            Console.WriteLine($"Published {numEvents}.");
            Console.ReadLine();
        }
    }
}