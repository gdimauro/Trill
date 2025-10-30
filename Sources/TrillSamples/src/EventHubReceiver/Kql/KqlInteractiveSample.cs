// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
#pragma warning disable SA1008 // Opening parenthesis must be spaced correctly
#pragma warning disable SA1013 // Closing brace must be followed by a space
#pragma warning disable SA1121 // Use built-in type alias

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using Microsoft.StreamProcessing;

namespace EventHubReceiver.Kql
{
    /// <summary>
    /// Interactive sample for KQL-based event processing with configurable simulation parameters
    /// Demonstrates real-time event generation and processing with various schemas and query types
    /// </summary>
    public static class KqlInteractiveSample
    {
        /// <summary>
        /// Run interactive KQL processor demo with user-specified parameters
        /// </summary>
        /// <param name="numberOfEvents">Number of events to generate (default: 100)</param>
        /// <param name="waitTimeBetweenEventsMs">Wait time between events in milliseconds (default: 50ms)</param>
        /// <param name="windowSizeSeconds">Window size for aggregation in seconds (default: 5)</param>
        /// <param name="slideSizeSeconds">Slide size for hopping window in seconds (default: 1)</param>
        /// <param name="queryType">Type of query to run (CountOnly, FieldAggregation, MultiMetric)</param>
        /// <param name="schemaType">Type of schema to use (Simple, Metrics, Sensors, Complex)</param>
        public static void Run(
            int numberOfEvents = 100,
            int waitTimeBetweenEventsMs = 50,
            int windowSizeSeconds = 5,
            int slideSizeSeconds = 1,
            KqlQueryType queryType = KqlQueryType.MultiMetric,
            SchemaType schemaType = SchemaType.Metrics)
        {
            Console.Clear();
            Console.WriteLine("╔════════════════════════════════════════════════════════════════════╗");
            Console.WriteLine("║          KQL Interactive Event Processing Simulation              ║");
            Console.WriteLine("╚════════════════════════════════════════════════════════════════════╝");
            Console.WriteLine();

            // Display configuration
            Console.WriteLine("Configuration:");
            Console.WriteLine($"  • Number of Events      : {numberOfEvents:N0}");
            Console.WriteLine($"  • Wait Between Events   : {waitTimeBetweenEventsMs}ms");
            Console.WriteLine($"  • Window Size           : {windowSizeSeconds}s");
            Console.WriteLine($"  • Slide Size            : {slideSizeSeconds}s");
            Console.WriteLine($"  • Query Type            : {queryType}");
            Console.WriteLine($"  • Schema Type           : {schemaType}");
            Console.WriteLine();

      // Create schema based on type
      var (schema, kqlSchemaText) = CreateSchema(schemaType);

      Console.WriteLine("KQL Schema:");
            Console.WriteLine(kqlSchemaText);
            Console.WriteLine($"Parsed: {schema}");
            Console.WriteLine();

            // Create query configuration
            var config = CreateQueryConfig(queryType, schemaType, windowSizeSeconds, slideSizeSeconds);

            // Setup checkpoint directory
            var checkpointDir = Path.Combine(
                Path.GetTempPath(),
                "KqlSimulation",
                $"{schemaType}-{queryType}-{DateTime.Now:yyyyMMdd-HHmmss}");

            Console.WriteLine($"Checkpoint Directory: {checkpointDir}");
            Console.WriteLine();

            // Create processor
            using var processor = new KqlEventProcessor("sim-partition-0", checkpointDir, schema, config);

            Console.WriteLine("Initializing processor...");
            processor.Initialize();
            Console.WriteLine("Processor initialized.");
            Console.WriteLine();

            // Generate and process events
            Console.WriteLine($"Generating and processing {numberOfEvents} events...");
            Console.WriteLine("Press Ctrl+C to stop.\n");

            var startTime = DateTime.UtcNow;
            var random = new Random(42);

            for (int i = 0; i < numberOfEvents; i++)
            {
                // Generate event based on schema type
                var record = GenerateEvent(schema, schemaType, startTime, i, random);

                // Create stream event
                var timestamp = startTime.AddMilliseconds(i * waitTimeBetweenEventsMs);
                var streamEvent = new StreamEvent<KqlDynamicRecord>(
                    timestamp.Ticks,
                    StreamEvent.InfinitySyncTime,
                    record);

                // Process event
                processor.ProcessEvent(streamEvent);

                // Console progress indicator
                if ((i + 1) % 10 == 0)
                {
                    Console.Write($"\rProcessed: {i + 1}/{numberOfEvents} events ({(double)(i + 1) / numberOfEvents:P0})");
                }

                // Wait between events
                if (waitTimeBetweenEventsMs > 0)
                {
                    Thread.Sleep(waitTimeBetweenEventsMs);
                }
            }

            Console.WriteLine($"\n\nAll {numberOfEvents} events processed.");
            Console.WriteLine("Flushing processor...");
            processor.Flush();

            Console.WriteLine("Waiting for final output windows...");
            Thread.Sleep(Math.Max(windowSizeSeconds * 1000, 2000));

            Console.WriteLine("\n✓ Simulation complete.");
            Console.WriteLine($"\nCheckpoint files saved to: {checkpointDir}");

            // Cleanup prompt
            Console.WriteLine("\nPress any key to clean up checkpoints or Ctrl+C to keep them...");
            Console.ReadKey(true);

            if (Directory.Exists(checkpointDir))
            {
                try
                {
                    Directory.Delete(checkpointDir, true);
                    Console.WriteLine("Checkpoints cleaned up.");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Could not delete checkpoints: {ex.Message}");
                }
            }
        }

        /// <summary>
        /// Run with menu-driven configuration
        /// </summary>
        public static void RunInteractive()
        {
            Console.Clear();
            Console.WriteLine("╔════════════════════════════════════════════════════════════════════╗");
            Console.WriteLine("║          KQL Interactive Event Processing Configuration           ║");
            Console.WriteLine("╚════════════════════════════════════════════════════════════════════╝");
            Console.WriteLine();

            // Get number of events
            Console.Write("Number of events to generate [100]: ");
            var eventsInput = Console.ReadLine();
            int numberOfEvents = string.IsNullOrWhiteSpace(eventsInput) ? 100 : int.Parse(eventsInput);

            // Get wait time
            Console.Write("Wait time between events in ms [50]: ");
            var waitInput = Console.ReadLine();
            int waitTime = string.IsNullOrWhiteSpace(waitInput) ? 50 : int.Parse(waitInput);

            // Get window size
            Console.Write("Window size in seconds [5]: ");
            var windowInput = Console.ReadLine();
            int windowSize = string.IsNullOrWhiteSpace(windowInput) ? 5 : int.Parse(windowInput);

            // Get slide size
            Console.Write("Slide size in seconds [1]: ");
            var slideInput = Console.ReadLine();
            int slideSize = string.IsNullOrWhiteSpace(slideInput) ? 1 : int.Parse(slideInput);

            // Select query type
            Console.WriteLine("\nQuery Type:");
            Console.WriteLine("  1 - Count Only");
            Console.WriteLine("  2 - Field Aggregation");
            Console.WriteLine("  3 - Multi-Metric (default)");
            Console.Write("Select [3]: ");
            var queryInput = Console.ReadLine();
            var queryType = string.IsNullOrWhiteSpace(queryInput) || queryInput == "3"
                ? KqlQueryType.MultiMetric
                : queryInput == "1" ? KqlQueryType.CountOnly : KqlQueryType.FieldAggregation;

            // Select schema type
            Console.WriteLine("\nSchema Type:");
            Console.WriteLine("  1 - Simple Events");
            Console.WriteLine("  2 - Metrics (default)");
            Console.WriteLine("  3 - Sensor Data");
            Console.WriteLine("  4 - Complex Events");
            Console.Write("Select [2]: ");
            var schemaInput = Console.ReadLine();
            var schemaType = string.IsNullOrWhiteSpace(schemaInput) || schemaInput == "2"
                ? SchemaType.Metrics
                : schemaInput == "1" ? SchemaType.Simple
                : schemaInput == "3" ? SchemaType.Sensors
                : SchemaType.Complex;

            Console.WriteLine("\nStarting simulation with your configuration...\n");
            Thread.Sleep(1000);

            Run(numberOfEvents, waitTime, windowSize, slideSize, queryType, schemaType);
        }

        /// <summary>
        /// Run predefined demo scenarios
        /// </summary>
        public static void RunDemoScenarios()
        {
            while (true)
            {
                Console.Clear();
                Console.WriteLine("╔════════════════════════════════════════════════════════════════════╗");
                Console.WriteLine("║              KQL Processor Demo Scenarios                          ║");
                Console.WriteLine("╚════════════════════════════════════════════════════════════════════╝");
                Console.WriteLine();
                Console.WriteLine("Select a demo scenario:");
                Console.WriteLine();
                Console.WriteLine("  1 - Quick Demo (50 events, fast)");
                Console.WriteLine("  2 - Standard Demo (100 events, moderate)");
                Console.WriteLine("  3 - Performance Test (1000 events, no delay)");
                Console.WriteLine("  4 - Real-time Simulation (200 events, 100ms delay)");
                Console.WriteLine("  5 - Sensor Network (500 events, sensor data)");
                Console.WriteLine("  6 - Custom Configuration");
                Console.WriteLine();
                Console.WriteLine("  0 - Return to Main Menu");
                Console.WriteLine();
                Console.Write("Your choice: ");

                var choice = Console.ReadLine();

                switch (choice)
                {
                    case "1":
                        Run(50, 20, 3, 1, KqlQueryType.MultiMetric, SchemaType.Metrics);
                        break;

                    case "2":
                        Run(100, 50, 5, 1, KqlQueryType.MultiMetric, SchemaType.Metrics);
                        break;

                    case "3":
                        Run(1000, 0, 5, 1, KqlQueryType.CountOnly, SchemaType.Simple);
                        break;

                    case "4":
                        Run(200, 100, 5, 2, KqlQueryType.FieldAggregation, SchemaType.Metrics);
                        break;

                    case "5":
                        Run(500, 30, 10, 2, KqlQueryType.MultiMetric, SchemaType.Sensors);
                        break;

                    case "6":
                        RunInteractive();
                        break;

                    case "0":
                        return;

                    default:
                        Console.WriteLine("Invalid choice. Press any key to continue...");
                        Console.ReadKey(true);
                        break;
                }

                if (choice != "0")
                {
                    Console.WriteLine("\nPress any key to continue...");
                    Console.ReadKey(true);
                }
            }
        }

        // ═════════════════════════════════════════════════════════════════════
        // Helper Methods
        // ═════════════════════════════════════════════════════════════════════

        private static (KqlTableSchema schema, string kqlText) CreateSchema(SchemaType schemaType)
        {
            string kqlText;

            switch (schemaType)
            {
                case SchemaType.Simple:
                    kqlText = @"
.create table Events (
    Timestamp: datetime,
    EventId: guid,
    EventType: string,
    Message: string
)";
                    break;

                case SchemaType.Metrics:
                    kqlText = @"
.create table Metrics (
    Timestamp: datetime,
    MetricName: string,
    Value: long,
    Unit: string,
    Source: string
)";
                    break;

                case SchemaType.Sensors:
                    kqlText = @"
.create table SensorData (
    Timestamp: datetime,
    SensorId: string,
    Temperature: long,
    Humidity: real,
    Pressure: real,
    Location: string
)";
                    break;

                case SchemaType.Complex:
                    kqlText = @"
.create table ComplexEvents (
    Timestamp: datetime,
    EventId: guid,
    UserId: string,
    IsActive: bool,
    Value: long,
    Score: real,
    Duration: timespan,
    Tags: dynamic
)";
                    break;

                default:
                    throw new ArgumentException($"Unknown schema type: {schemaType}");
            }

            var schema = KqlSchemaParser.Parse(kqlText);
            return (schema, kqlText);
        }

        private static KqlQueryConfig CreateQueryConfig(
            KqlQueryType queryType,
            SchemaType schemaType,
            int windowSizeSeconds,
            int slideSizeSeconds)
        {
            var windowSize = TimeSpan.FromSeconds(windowSizeSeconds);
            var slideSize = TimeSpan.FromSeconds(slideSizeSeconds);

            // Determine aggregation field based on schema type
            string aggregationField = schemaType switch
            {
                SchemaType.Metrics => "Value",
                SchemaType.Sensors => "Temperature",
                SchemaType.Complex => "Value",
                _ => null
            };

            return queryType switch
            {
                KqlQueryType.CountOnly => KqlQueryConfig.CreateDefault(),
                KqlQueryType.FieldAggregation => KqlQueryConfig.CreateFieldAggregation(aggregationField, windowSize, slideSize),
                KqlQueryType.MultiMetric => KqlQueryConfig.CreateMultiMetric(aggregationField, windowSize, slideSize),
                _ => KqlQueryConfig.CreateDefault()
            };
        }

        private static KqlDynamicRecord GenerateEvent(
            KqlTableSchema schema,
            SchemaType schemaType,
            DateTime startTime,
            int index,
            Random random)
        {
            var timestamp = startTime.AddSeconds(index * 0.1);

            switch (schemaType)
            {
                case SchemaType.Simple:
                    return new KqlDynamicRecordBuilder(schema)
                        .Set("Timestamp", timestamp)
                        .Set("EventId", Guid.NewGuid())
                        .Set("EventType", new[] { "Info", "Warning", "Error" }[random.Next(3)])
                        .Set("Message", $"Event message #{index}")
                        .Build();

                case SchemaType.Metrics:
                    return new KqlDynamicRecordBuilder(schema)
                        .Set("Timestamp", timestamp)
                        .Set("MetricName", new[] { "CPU", "Memory", "Disk", "Network" }[random.Next(4)])
                        .Set("Value", (long)random.Next(10, 100))
                        .Set("Unit", new[] { "percent", "MB", "GB", "Mbps" }[random.Next(4)])
                        .Set("Source", $"Server-{random.Next(1, 6)}")
                        .Build();

                case SchemaType.Sensors:
                    return new KqlDynamicRecordBuilder(schema)
                        .Set("Timestamp", timestamp)
                        .Set("SensorId", $"Sensor-{(char)('A' + random.Next(0, 5))}")
                        .Set("Temperature", (long)random.Next(15, 35))
                        .Set("Humidity", random.NextDouble() * 100)
                        .Set("Pressure", 980 + random.NextDouble() * 50)
                        .Set("Location", $"Building-{random.Next(1, 4)}")
                        .Build();

                case SchemaType.Complex:
                    var tags = new Dictionary<string, object>
                    {
                        { "Category", $"Cat-{random.Next(1, 4)}" },
                        { "Priority", random.Next(1, 6) },
                        { "Region", $"Region-{(char)('A' + random.Next(0, 3))}" }
                    };

                    return new KqlDynamicRecordBuilder(schema)
                        .Set("Timestamp", timestamp)
                        .Set("EventId", Guid.NewGuid())
                        .Set("UserId", $"User-{random.Next(1, 20)}")
                        .Set("IsActive", random.Next(2) == 0)
                        .Set("Value", (long)random.Next(100, 1000))
                        .Set("Score", random.NextDouble() * 100)
                        .Set("Duration", TimeSpan.FromSeconds(random.Next(1, 300)))
                        .Set("Tags", tags)
                        .Build();

                default:
                    throw new ArgumentException($"Unknown schema type: {schemaType}");
            }
        }
    }

    /// <summary>
    /// Types of schemas available for simulation
    /// </summary>
    public enum SchemaType
    {
        Simple,
        Metrics,
        Sensors,
        Complex
    }
}
