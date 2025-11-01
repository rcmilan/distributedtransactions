using System;
using System.Text.RegularExpressions;
using Humanizer;

namespace DT.Shared.Events
{
    public static class EventMetadata
    {
        public static string GetExchangeName(Type eventType)
        {
            string name = eventType.Name;
            if (!name.EndsWith("Event"))
                throw new ArgumentException("Event type name must end with 'Event'");

            string withoutEvent = name.Substring(0, name.Length - 5);
            var match = Regex.Match(withoutEvent, @"([A-Z][a-z]*)$");
            if (!match.Success)
                throw new ArgumentException("Invalid event type name format");

            string eventTypeStr = match.Value;
            string domain = withoutEvent.Substring(0, withoutEvent.Length - eventTypeStr.Length);
            return domain.Pluralize().ToLower();
        }

        public static string GetRoutingKey(Type eventType)
        {
            string name = eventType.Name;
            if (!name.EndsWith("Event"))
                throw new ArgumentException("Event type name must end with 'Event'");

            string withoutEvent = name.Substring(0, name.Length - 5);
            var match = Regex.Match(withoutEvent, @"([A-Z][a-z]*)$");
            if (!match.Success)
                throw new ArgumentException("Invalid event type name format");

            string eventTypeStr = match.Value;
            string domain = withoutEvent.Substring(0, withoutEvent.Length - eventTypeStr.Length);
            return domain.ToLower() + "." + eventTypeStr.ToLower();
        }
    }
}