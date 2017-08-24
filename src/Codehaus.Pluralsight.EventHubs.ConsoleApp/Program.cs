using System;
using Microsoft.ServiceBus.Messaging;

namespace Codehaus.Pluralsight.EventHubs.ConsoleApp
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

            #region Simulate Many

            var random = new Random(Environment.TickCount);

            for (var i = 0; i < 1000; i++)
            {
                var deviceTelemetry = DeviceTelemetry.GenerateRandom(random);
                publisher.Publish(deviceTelemetry);
            }

            #endregion

            #region Publish DeviceTelemetry

            //var deviceTelemetry = new DeviceTelemetry
            //{
            //    DeviceType = DeviceType.Phone,
            //    IpAddress = "127.0.0.1",
            //    IsOn = true,
            //    Time = DateTime.Now
            //};

            //publisher.Publish(deviceTelemetry);

            #endregion

            //var strings = new List<string> {"Hello", "World"};
            //publisher.Publish(strings);

            var eventProcessorHostName = Guid.NewGuid().ToString();
            var eventProcessorHost = new EventProcessorHost(eventProcessorHostName, eventHubName,
                EventHubConsumerGroup.DefaultGroupName, eventHubConnectionString, storageConnectionString);
            Console.WriteLine("Registering EventProcessor...");
            var options = new EventProcessorOptions();
            options.ExceptionReceived += (sender, e) => { Console.WriteLine(e.Exception); };
            eventProcessorHost.RegisterEventProcessorAsync<Consumer>(options).Wait();

            Console.WriteLine("Receiving. Press enter key to stop worker...");
            Console.ReadLine();
            eventProcessorHost.UnregisterEventProcessorAsync().Wait();
        }
    }
}