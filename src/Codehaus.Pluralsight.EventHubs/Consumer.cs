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
            Console.WriteLine("Processor Shutting Down. Partition '{0}', Reason: '{1}'.", context.Lease.PartitionId,
                reason);
            if (reason == CloseReason.Shutdown)
                await context.CheckpointAsync();
        }

        Task IEventProcessor.OpenAsync(PartitionContext context)
        {
            Console.WriteLine("SimpleEventProcessor initialized.  Partition: '{0}', Offset: '{1}'",
                context.Lease.PartitionId, context.Lease.Offset);
            _checkpointStopWatch = new Stopwatch();
            _checkpointStopWatch.Start();
            return Task.FromResult<object>(null);
        }

        async Task IEventProcessor.ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> messages)
        {
            foreach (var eventData in messages)
            {
                // 1. Convert the stream of bytes to string.
                var data = Encoding.UTF8.GetString(eventData.GetBytes());

                #region Deserialize device telemetry

                // 2. Deserialize the object.
                var deviceTelemetry = JsonConvert.DeserializeObject<DeviceTelemetry>(data);
                Console.WriteLine("{0} published at {1:R}. Status: {2}",
                    deviceTelemetry.DeviceType,
                    deviceTelemetry.Time,
                    deviceTelemetry.IsOn ? "On" : "Off");
                Console.WriteLine($"Downloaded {++_counter} events");

                // Todo: Scale out Consumers.

                #endregion

                // Console.WriteLine("Message received.  Partition: '{0}', Data: '{1}'", context.Lease.PartitionId, data);
            }

            if (_checkpointStopWatch.Elapsed > TimeSpan.FromMinutes(5))
            {
                await context.CheckpointAsync();
                _checkpointStopWatch.Restart();
            }
        }
    }
}