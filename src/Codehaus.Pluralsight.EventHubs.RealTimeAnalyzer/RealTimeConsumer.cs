using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;
using Newtonsoft.Json;

namespace Codehaus.Pluralsight.EventHubs.RealTimeAnalyzer
{
    internal class RealTimeConsumer : IEventProcessor
    {
        private readonly List<DeviceTelemetry> _cache = new List<DeviceTelemetry>();
        private Stopwatch _checkpointStopWatch;

        async Task IEventProcessor.CloseAsync(PartitionContext context, CloseReason reason)
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine("Releasing lease on Partition {0}. Reason: {1}.", context.Lease.PartitionId, reason);
            if (reason == CloseReason.Shutdown)
                await context.CheckpointAsync();
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
                var data = Encoding.UTF8.GetString(eventData.GetBytes());

                var deviceTelemetry = JsonConvert.DeserializeObject<DeviceTelemetry>(data);
                _cache.Add(deviceTelemetry);
            }

            var totalNumDevices = _cache.Count;
            var numDevicesSwitchedOn = _cache.Count(dt => dt.IsOn);           
            Console.WriteLine($"{numDevicesSwitchedOn} of {totalNumDevices} devices are currently switched on.");

            if (_checkpointStopWatch.Elapsed > TimeSpan.FromMinutes(5))
            {
                await context.CheckpointAsync();
                _checkpointStopWatch.Restart();
            }
        }
    }
}