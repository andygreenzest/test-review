using System;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.IO;

namespace test_review.ServiceBusExplorer
{
    // Model classes for JSON representation
    public class RuleProperties
    {
        public required string FilterType { get; set; }
        public CorrelationFilterProperties? CorrelationFilter { get; set; }
    }

    public class CorrelationFilterProperties
    {
        public string? ContentType { get; set; }
        public required Dictionary<string, string> Properties { get; set; }
    }

    public class ServiceBusRule
    {
        public required string Name { get; set; }
        public required RuleProperties Properties { get; set; }
    }

    public class SubscriptionProperties
    {
        public bool DeadLetteringOnMessageExpiration { get; set; }
        public required string DefaultMessageTimeToLive { get; set; }
        public required string LockDuration { get; set; }
        public int MaxDeliveryCount { get; set; }
        public string? ForwardDeadLetteredMessagesTo { get; set; }
        public string? ForwardTo { get; set; }
        public bool RequiresSession { get; set; }
    }

    public class ServiceBusSubscription
    {
        public required string Name { get; set; }
        public required SubscriptionProperties Properties { get; set; }
        public List<ServiceBusRule> Rules { get; set; } = new List<ServiceBusRule>();
    }

    public class TopicProperties
    {
        public required string DefaultMessageTimeToLive { get; set; }
        public required string DuplicateDetectionHistoryTimeWindow { get; set; }
        public bool RequiresDuplicateDetection { get; set; }
    }

    public class ServiceBusTopic
    {
        public required string Name { get; set; }
        public required TopicProperties Properties { get; set; }
        public List<ServiceBusSubscription> Subscriptions { get; set; } = new List<ServiceBusSubscription>();
    }

    public class ServiceBusNamespace
    {
        public required string Name { get; set; }
        public List<ServiceBusTopic> Topics { get; set; } = new List<ServiceBusTopic>();
    }

    public class LoggingConfig
    {
        public string Type { get; set; } = "File";
    }

    public class UserConfig
    {
        public List<ServiceBusNamespace> Namespaces { get; set; } = new List<ServiceBusNamespace>();
        public LoggingConfig Logging { get; set; } = new LoggingConfig();
    }

    public class ServiceBusExplorer
    {
        public UserConfig UserConfig { get; set; } = new UserConfig();
    }

    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("Service Bus Topics and Subscriptions Explorer");
            Console.WriteLine("--------------------------------------------");
            
            string connectionString;
            
            // Check if connection string is passed as argument or prompt for it
            if (args.Length > 0)
            {
                connectionString = args[0];
            }
            else
            {
                Console.Write("Enter Azure Service Bus connection string: ");
                connectionString = Console.ReadLine();
            }
            
            if (string.IsNullOrWhiteSpace(connectionString))
            {
                Console.WriteLine("Error: Connection string cannot be empty.");
                return;
            }
            
            try
            {
                // Create a ServiceBusAdministrationClient to manage entities
                var adminClient = new ServiceBusAdministrationClient(connectionString);
                
                // Create the object model to hold the Service Bus topology
                var explorer = new ServiceBusExplorer();
                
                // Get all topics
                Console.WriteLine("\nRetrieving topics...");
                var topicNames = new List<string>();
                
                await foreach (var topic in adminClient.GetTopicsAsync())
                {
                    topicNames.Add(topic.Name);
                }
                
                if (topicNames.Count == 0)
                {
                    Console.WriteLine("No topics found in the namespace.");
                    OutputJson(explorer);
                    return;
                }
                
                Console.WriteLine($"Found {topicNames.Count} topics.");
                var namespace1 = new ServiceBusNamespace { Name = "DefaultNamespace" };
                
                // For each topic, get its subscriptions
                foreach (var topicName in topicNames)
                {
                    var topicProperties = await adminClient.GetTopicAsync(topicName);
                    var topicInfo = new ServiceBusTopic 
                    { 
                        Name = topicName,
                        Properties = new TopicProperties
                        {
                            DefaultMessageTimeToLive = topicProperties.Value.DefaultMessageTimeToLive.ToString(),
                            DuplicateDetectionHistoryTimeWindow = topicProperties.Value.DuplicateDetectionHistoryTimeWindow.ToString(),
                            RequiresDuplicateDetection = topicProperties.Value.RequiresDuplicateDetection
                        }
                    };
                    
                    await foreach (var subscription in adminClient.GetSubscriptionsAsync(topicName))
                    {
                        var subscriptionInfo = new ServiceBusSubscription
                        {
                            Name = subscription.SubscriptionName,
                            Properties = new SubscriptionProperties
                            {
                                DeadLetteringOnMessageExpiration = subscription.DeadLetteringOnMessageExpiration,
                                DefaultMessageTimeToLive = subscription.DefaultMessageTimeToLive.ToString(),
                                LockDuration = subscription.LockDuration.ToString(),
                                MaxDeliveryCount = subscription.MaxDeliveryCount,
                                ForwardDeadLetteredMessagesTo = subscription.ForwardDeadLetteredMessagesTo ?? "",
                                ForwardTo = subscription.ForwardTo ?? "",
                                RequiresSession = subscription.RequiresSession
                            }
                        };
                        
                        // Get subscription rules
                        await foreach (var rule in adminClient.GetRulesAsync(topicName, subscription.SubscriptionName))
                        {
#pragma warning disable CS8619 // Nullability of reference types in value doesn't match target type.
                            var ruleInfo = new ServiceBusRule
                            {
                                Name = rule.Name,
                                Properties = new RuleProperties
                                {
                                    FilterType = rule.Filter.GetType().Name.Replace("Filter", ""),
                                    CorrelationFilter = rule.Filter is CorrelationRuleFilter correlationFilter ? new CorrelationFilterProperties
                                    {
                                        ContentType = correlationFilter.ContentType,
                                        Properties = correlationFilter.ApplicationProperties?.ToDictionary(kvp => kvp.Key, kvp => kvp.Value?.ToString()) ?? new Dictionary<string, string>()
                                    } : null
                                }
                            };
#pragma warning restore CS8619 // Nullability of reference types in value doesn't match target type.

                            subscriptionInfo.Rules.Add(ruleInfo);
                        }
                        
                        topicInfo.Subscriptions.Add(subscriptionInfo);
                    }
                    
                    namespace1.Topics.Add(topicInfo);
                }
                
                explorer.UserConfig.Namespaces.Add(namespace1);
                explorer.UserConfig.Logging = new LoggingConfig { Type = "File" };
                
                // Output the entire structure as JSON
                OutputJson(explorer);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
                Console.WriteLine(ex.ToString());
            }
        }
        
        private static void OutputJson(ServiceBusExplorer explorer)
        {
            var options = new JsonSerializerOptions
            {
                WriteIndented = true
            };
            
            string jsonString = JsonSerializer.Serialize(explorer, options);
            File.WriteAllText("serviceBusExplorer.json", jsonString);
            File.WriteAllText("emulator.json", jsonString);
        }
    }
}
