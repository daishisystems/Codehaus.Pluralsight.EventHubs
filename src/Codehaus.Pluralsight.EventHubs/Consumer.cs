using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;
using Newtonsoft.Json;

namespace Codehaus.Pluralsight.EventHubs
{
    public class Consumer : IEventProcessor
    {
        private Stopwatch _checkpointStopWatch;
        private volatile int _counter;

        async Task IEventProcessor.CloseAsync(PartitionContext context, CloseReason reason)
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine("Releasing lease on Partition {0}. Reason: {1}.", context.Lease.PartitionId, reason);
            if (reason == CloseReason.Shutdown)
            {
                await context.CheckpointAsync();
                Console.WriteLine($"Downloaded {_counter} events");
            }
            Console.ReadLine();
        }

        Task IEventProcessor.OpenAsync(PartitionContext context)
        {
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine("I am listening to Partition {0}, at position {1}.",
                context.Lease.PartitionId, context.Lease.Offset);
            _checkpointStopWatch = new Stopwatch();
            _checkpointStopWatch.Start();
            return Task.FromResult<object>(null);
        }

        async Task IEventProcessor.ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> messages)
        {
            Console.ForegroundColor = ConsoleColor.Green;
            foreach (var eventData in messages)
            {
                // 1. Convert the stream of bytes to string.
                var data = Encoding.UTF8.GetString(eventData.GetBytes());

                // 2. Deserialize the object.
                var deviceTelemetry = JsonConvert.DeserializeObject<DeviceTelemetry>(data);
                Console.WriteLine("{0} published at {1:R}. Status: {2}",
                    deviceTelemetry.DeviceType,
                    deviceTelemetry.Time,
                    deviceTelemetry.IsOn ? "On" : "Off");
                _counter++;
            }

            if (_checkpointStopWatch.Elapsed > TimeSpan.FromMinutes(5))
            {
                await context.CheckpointAsync();
                _checkpointStopWatch.Restart();
            }
        }
    }
}