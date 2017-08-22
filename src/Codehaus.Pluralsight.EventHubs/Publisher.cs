using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;
using Newtonsoft.Json;

namespace Codehaus.Pluralsight.EventHubs
{
    public class Publisher
    {
        private EventHubClient _eventHubClient;

        public void Init(string connectionString)
        {
            if (string.IsNullOrEmpty(connectionString)) throw new ArgumentNullException(nameof(connectionString));
            _eventHubClient = EventHubClient.CreateFromConnectionString(connectionString);
        }

        public void Publish<T>(T myEvent)
        {
            if (myEvent == null) throw new ArgumentNullException(nameof(myEvent));

            // 1. Serialize the event
            var serializedEvent = JsonConvert.SerializeObject(myEvent);

            // 2. Convert serialized event to bytes
            var eventBytes = Encoding.UTF8.GetBytes(serializedEvent);

            // 3. Wrap event bytes in EventData instance.
            var eventData = new EventData(eventBytes);

            // 4. Publish the event
            _eventHubClient.Send(eventData);
        }

        public async Task PublishAsync<T>(T myEvent)
        {
            if (myEvent == null) throw new ArgumentNullException(nameof(myEvent));

            // 1. Serialize the event
            var serializedEvent = JsonConvert.SerializeObject(myEvent);

            // 2. Convert serialized event to bytes
            var eventBytes = Encoding.UTF8.GetBytes(serializedEvent);

            // 3. Wrap event bytes in EventData instance.
            var eventData = new EventData(eventBytes);

            // 4. Publish the event
            await _eventHubClient.SendAsync(eventData);
        }

        public void Publish<T>(IEnumerable<T> myEvents)
        {
            if (myEvents == null) throw new ArgumentNullException(nameof(myEvents));

            var events = new List<EventData>();

            foreach (var myEvent in myEvents)
            {
                // 1. Serialize each event
                var serializedEvent = JsonConvert.SerializeObject(myEvent);

                // 2. Convert serialized event to bytes
                var eventBytes = Encoding.UTF8.GetBytes(serializedEvent);

                // 3. Wrap event bytes in EventData instance.
                events.Add(new EventData(eventBytes));
            }

            // 4. Publish the events
            _eventHubClient.SendBatch(events);
        }

        public async Task PublishAsync<T>(IEnumerable<T> myEvents)
        {
            if (myEvents == null) throw new ArgumentNullException(nameof(myEvents));

            var events = new List<EventData>();

            foreach (var myEvent in myEvents)
            {
                // 1. Serialize each event
                var serializedEvent = JsonConvert.SerializeObject(myEvent);

                // 2. Convert serialized event to bytes
                var eventBytes = Encoding.UTF8.GetBytes(serializedEvent);

                // 3. Wrap event bytes in EventData instance.
                events.Add(new EventData(eventBytes));
            }

            // 4. Publish the events
            await _eventHubClient.SendBatchAsync(events);
        }
    }
}