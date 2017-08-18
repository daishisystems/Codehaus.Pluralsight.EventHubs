using System;
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

        public void Publish(string myEvent)
        {
            // 1. Serialize the event
            var serializedEvent = JsonConvert.SerializeObject(myEvent);

            // 2. Convert serialized event to bytes
            var eventBytes = Encoding.UTF8.GetBytes(serializedEvent);

            // 3. Wrap event bytes in EventData instance.
            var eventData = new EventData(eventBytes);

            // 4. Publish the event
            _eventHubClient.Send(eventData);
        }

        public async Task PublishAsync(string myEvent)
        {
            // 1. Serialize the event
            var serializedEvent = JsonConvert.SerializeObject(myEvent);

            // 2. Convert serialized event to bytes
            var eventBytes = Encoding.UTF8.GetBytes(serializedEvent);

            // 3. Wrap event bytes in EventData instance.
            var eventData = new EventData(eventBytes);

            // 4. Publish the event
            await _eventHubClient.SendAsync(eventData);
        }
    }
}