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
using System.Linq;
using System.Threading;
using EventHubReceiver.Kql;
using Microsoft.StreamProcessing;

namespace EventHubReceiver
{
    /// <summary>
    /// Sample demonstrating KQL schema-based event processing with Trill
    /// Shows how to define schemas using KQL syntax and process events with checkpointing
    /// </summary>
    public static class KqlProcessorSample
    {
        /// <summary>
        /// Run KQL processor with configurable parameters (matches pattern of other samples)
        /// </summary>
        /// <param name="numberOfEvents">Number of events to generate</param>
        /// <param name="waitTimeBetweenEventsMs">Wait time between events in milliseconds</param>
        /// <param name="windowSizeSeconds">Window size for aggregation in seconds</param>
        /// <param name="slideSizeSeconds">Slide size for hopping window in seconds</param>
        /// <param name="schemaType">Type of schema to use (1=Simple, 2=Metrics, 3=Sensors, 4=Complex)</param>
        /// <param name="queryType">Type of query (1=CountOnly, 2=FieldAggregation, 3=MultiMetric)</param>
        public static void RunWithParameters(
            int numberOfEvents = 100,
            int waitTimeBetweenEventsMs = 50,
            int windowSizeSeconds = 5,
            int slideSizeSeconds = 1,
            int schemaType = 2,
            int queryType = 3)
        {
            Console.Clear();
            Console.WriteLine("╔════════════════════════════════════════════════════════════════════╗");
            Console.WriteLine("║     KQL Event Processing with Checkpoint Demonstration            ║");
            Console.WriteLine("╚════════════════════════════════════════════════════════════════════╝");
            Console.WriteLine();

            // Display configuration
            Console.WriteLine("Configuration:");
            Console.WriteLine($"  • Number of Events      : {numberOfEvents:N0}");
            Console.WriteLine($"  • Wait Between Events   : {waitTimeBetweenEventsMs}ms");
            Console.WriteLine($"  • Window Size           : {windowSizeSeconds}s");
            Console.WriteLine($"  • Slide Size            : {slideSizeSeconds}s");
            Console.WriteLine($"  • Schema Type           : {GetSchemaTypeName(schemaType)}");
            Console.WriteLine($"  • Query Type            : {GetQueryTypeName(queryType)}");
            Console.WriteLine();

            // Create schema and config based on types
            var (schema, kqlSchemaText) = CreateSchemaByType(schemaType);
            var config = CreateConfigByType(queryType, schemaType, windowSizeSeconds, slideSizeSeconds);

            Console.WriteLine("KQL Schema:");
            Console.WriteLine(kqlSchemaText);
            Console.WriteLine($"Parsed: {schema}");
            Console.WriteLine();

            // Setup checkpoint directory with timestamp to demonstrate restore
            var checkpointDir = Path.Combine(
                Path.GetTempPath(),
                "KqlCheckpoints",
                $"Run-{GetSchemaTypeName(schemaType)}-{DateTime.Now:yyyyMMdd-HHmmss}");

            Console.WriteLine($"Checkpoint Directory: {checkpointDir}");
            Console.WriteLine();

            // PHASE 1: Process first batch and checkpoint
            Console.WriteLine("═══ PHASE 1: Initial Processing ═══");
            Console.WriteLine($"Processing first {numberOfEvents / 2} events...");
            Console.WriteLine();

            using (var processor = new KqlEventProcessor("sim-partition", checkpointDir, schema, config))
            {
                processor.Initialize();

                var startTime = DateTime.UtcNow;
                var random = new Random(42);

                // Process first half of events
                for (int i = 0; i < numberOfEvents / 2; i++)
                {
                    var record = GenerateEventByType(schema, schemaType, startTime, i, random);
                    var timestamp = startTime.AddMilliseconds(i * waitTimeBetweenEventsMs);

                    var streamEvent = new StreamEvent<KqlDynamicRecord>(
                        timestamp.Ticks,
                        StreamEvent.InfinitySyncTime,
                        record);

                    processor.ProcessEvent(streamEvent);

                    if ((i + 1) % 10 == 0)
                    {
                        Console.Write($"\rProcessed: {i + 1}/{numberOfEvents / 2} events");
                    }

                    if (waitTimeBetweenEventsMs > 0)
                    {
                        Thread.Sleep(waitTimeBetweenEventsMs);
                    }
                }

                Console.WriteLine("\n\nFlushing processor...");
                processor.Flush();
                Thread.Sleep(1000);

                Console.WriteLine("Waiting for checkpoint to be taken (happens every 10 seconds)...");
                Thread.Sleep(11000); // Wait for checkpoint

                Console.WriteLine("✓ Phase 1 complete. Checkpoint should be saved.");
            }

            Console.WriteLine("\n" + new string('─', 72));
            Console.WriteLine("Processor disposed. Checkpoint files should exist:");

            if (Directory.Exists(checkpointDir))
            {
                var checkpointFiles = Directory.GetFiles(checkpointDir, "*.checkpoint");
                var metadataFiles = Directory.GetFiles(checkpointDir, "*.metadata");

                Console.WriteLine($"  • Checkpoint files: {checkpointFiles.Length}");
                Console.WriteLine($"  • Metadata files  : {metadataFiles.Length}");

                if (checkpointFiles.Length > 0)
                {
                    Console.WriteLine($"  • Latest checkpoint: {Path.GetFileName(checkpointFiles[0])}");
                }
            }

            Console.WriteLine("\nPress any key to continue to Phase 2 (restore from checkpoint)...");
            Console.ReadKey(true);

            // PHASE 2: Restore from checkpoint and process remaining events
            Console.WriteLine("\n═══ PHASE 2: Restore from Checkpoint ═══");
            Console.WriteLine($"Restoring from checkpoint and processing remaining {numberOfEvents / 2} events...");
            Console.WriteLine();

            using (var processor = new KqlEventProcessor("sim-partition", checkpointDir, schema, config))
            {
                // Initialize will automatically restore from checkpoint
                processor.Initialize();

                var startTime = DateTime.UtcNow;
                var random = new Random(42); // Same seed to continue sequence

                // Skip first half (already processed), generate second half
                for (int i = 0; i < numberOfEvents / 2; i++)
                {
                    // Generate but skip (to advance random state)
                    GenerateEventByType(schema, schemaType, startTime, i, random);
                }

                // Now process second half
                for (int i = numberOfEvents / 2; i < numberOfEvents; i++)
                {
                    var record = GenerateEventByType(schema, schemaType, startTime, i, random);
                    var timestamp = startTime.AddMilliseconds(i * waitTimeBetweenEventsMs);

                    var streamEvent = new StreamEvent<KqlDynamicRecord>(
                        timestamp.Ticks,
                        StreamEvent.InfinitySyncTime,
                        record);

                    processor.ProcessEvent(streamEvent);

                    if ((i + 1) % 10 == 0)
                    {
                        Console.Write($"\rProcessed: {i + 1 - numberOfEvents / 2}/{numberOfEvents / 2} events");
                    }

                    if (waitTimeBetweenEventsMs > 0)
                    {
                        Thread.Sleep(waitTimeBetweenEventsMs);
                    }
                }

                Console.WriteLine("\n\nFlushing processor...");
                processor.Flush();
                Thread.Sleep(Math.Max(windowSizeSeconds * 1000, 2000));

                Console.WriteLine("✓ Phase 2 complete.");
            }

            Console.WriteLine("\n" + new string('═', 72));
            Console.WriteLine("Simulation complete!");
            Console.WriteLine($"Total events processed: {numberOfEvents}");
            Console.WriteLine($"Checkpoint directory: {checkpointDir}");

            // Cleanup prompt
            Console.WriteLine("\nPress 'D' to delete checkpoints, or any other key to keep them...");
            var key = Console.ReadKey(true);

            if (key.Key == ConsoleKey.D && Directory.Exists(checkpointDir))
            {
                try
                {
                    Directory.Delete(checkpointDir, true);
                    Console.WriteLine("Checkpoints deleted.");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Could not delete checkpoints: {ex.Message}");
                }
            }
            else
            {
                Console.WriteLine("Checkpoints preserved for inspection.");
            }
        }

        public static void Run()
        {
            Console.WriteLine("╔════════════════════════════════════════════════════════════════════╗");
            Console.WriteLine("║          KQL Schema-Based Event Processing Sample                 ║");
            Console.WriteLine("╚════════════════════════════════════════════════════════════════════╝");
            Console.WriteLine();

            // Example 1: Simple event counting with KQL schema
            RunSimpleCountExample();
            Console.WriteLine("\n" + new string('═', 72) + "\n");

            // Example 2: Field aggregation
            RunFieldAggregationExample();
            Console.WriteLine("\n" + new string('═', 72) + "\n");

            // Example 3: Multi-metric analysis
            RunMultiMetricExample();
            Console.WriteLine("\n" + new string('═', 72) + "\n");

            // Example 4: Complex schema with checkpoint demonstration
            RunComplexSchemaExample();
        }

        /// <summary>
        /// Example 1: Simple event counting with KQL schema
        /// </summary>
        private static void RunSimpleCountExample()
        {
            Console.WriteLine("Example 1: Simple Event Counting");
            Console.WriteLine("─────────────────────────────────");

            // Define schema using KQL syntax
            var kqlSchema = @"
.create table SimpleEvents (
    Timestamp: datetime,
    EventId: guid,
    Message: string
)";

            Console.WriteLine($"KQL Schema:\n{kqlSchema}\n");

            // Parse the schema
            var schema = KqlSchemaParser.Parse(kqlSchema);
            Console.WriteLine($"Parsed Schema: {schema}\n");

            // Create processor
            var checkpointDir = Path.Combine(Path.GetTempPath(), "KqlCheckpoints", "Simple");
            var config = KqlQueryConfig.CreateDefault();

            using var processor = new KqlEventProcessor("partition-0", checkpointDir, schema, config);
            processor.Initialize();

            // Generate and process events
            var startTime = DateTime.UtcNow;
            var events = GenerateSimpleEvents(schema, startTime, 20);

            Console.WriteLine($"Processing {events.Count} events...\n");

            foreach (var evt in events)
            {
                processor.ProcessEvent(evt);
            }

            processor.Flush();
            Thread.Sleep(1000); // Wait for output
        }

        /// <summary>
        /// Example 2: Field aggregation on numeric column
        /// </summary>
        private static void RunFieldAggregationExample()
        {
            Console.WriteLine("Example 2: Field Aggregation");
            Console.WriteLine("────────────────────────────");

            var kqlSchema = @"
.create table MetricEvents (
    Timestamp: datetime,
    MetricName: string,
    Value: long,
    Source: string
)";

            Console.WriteLine($"KQL Schema:\n{kqlSchema}\n");

            var schema = KqlSchemaParser.Parse(kqlSchema);
            var checkpointDir = Path.Combine(Path.GetTempPath(), "KqlCheckpoints", "FieldAggregation");
            var config = KqlQueryConfig.CreateFieldAggregation("Value");

            using var processor = new KqlEventProcessor("partition-0", checkpointDir, schema, config);
            processor.Initialize();

            var startTime = DateTime.UtcNow;
            var events = GenerateMetricEvents(schema, startTime, 30);

            Console.WriteLine($"Processing {events.Count} metric events...\n");

            foreach (var evt in events)
            {
                processor.ProcessEvent(evt);
            }

            processor.Flush();
            Thread.Sleep(1000);
        }

        /// <summary>
        /// Example 3: Multi-metric analysis with min/max/avg
        /// </summary>
        private static void RunMultiMetricExample()
        {
            Console.WriteLine("Example 3: Multi-Metric Analysis");
            Console.WriteLine("─────────────────────────────────");

            var kqlSchema = @"
.create table SensorData (
    Timestamp: datetime,
    SensorId: string,
    Temperature: long,
    Humidity: real,
    Location: string
)";

            Console.WriteLine($"KQL Schema:\n{kqlSchema}\n");

            var schema = KqlSchemaParser.Parse(kqlSchema);
            var checkpointDir = Path.Combine(Path.GetTempPath(), "KqlCheckpoints", "MultiMetric");
            var config = KqlQueryConfig.CreateMultiMetric("Temperature", TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(1));

            using var processor = new KqlEventProcessor("partition-0", checkpointDir, schema, config);
            processor.Initialize();

            var startTime = DateTime.UtcNow;
            var events = GenerateSensorEvents(schema, startTime, 40);

            Console.WriteLine($"Processing {events.Count} sensor events...\n");

            foreach (var evt in events)
            {
                processor.ProcessEvent(evt);
            }

            processor.Flush();
            Thread.Sleep(2000);
        }

        /// <summary>
        /// Example 4: Complex schema with checkpoint demonstration
        /// Now prompts for parameters to match DynamicReceiver pattern
        /// </summary>
        private static void RunComplexSchemaExample()
        {
            Console.WriteLine("Example 4: Complex Schema with Checkpoint Demo");
            Console.WriteLine("───────────────────────────────────────────────");
            Console.WriteLine();
            Console.WriteLine("This example demonstrates checkpoint persistence and restoration.");
            Console.WriteLine("The simulation will:");
            Console.WriteLine("  1. Process first batch of events and create a checkpoint");
            Console.WriteLine("  2. Dispose the processor");
            Console.WriteLine("  3. Create a new processor that restores from the checkpoint");
            Console.WriteLine("  4. Process the remaining events");
            Console.WriteLine();

            // Prompt for parameters like DynamicReceiver
            Console.Write("Enter window size in seconds [default: 5]: ");
            var windowInput = Console.ReadLine();
            var windowSize = string.IsNullOrWhiteSpace(windowInput) ? 5 : int.Parse(windowInput);

            Console.Write("Enter slide size in seconds [default: 1]: ");
            var slideInput = Console.ReadLine();
            var slideSize = string.IsNullOrWhiteSpace(slideInput) ? 1 : int.Parse(slideInput);

            Console.Write("Enter number of events to generate [default: 50]: ");
            var countInput = Console.ReadLine();
            var eventCount = string.IsNullOrWhiteSpace(countInput) ? 50 : int.Parse(countInput);

            Console.Write("Enter event generation rate (ms delay) [default: 100]: ");
            var rateInput = Console.ReadLine();
            var generationRate = string.IsNullOrWhiteSpace(rateInput) ? 100 : int.Parse(rateInput);

            Console.WriteLine();
            Console.WriteLine("Press any key to start...");
            Console.ReadKey(true);

            // Use provided parameters
            RunWithParameters(
                numberOfEvents: eventCount,
                waitTimeBetweenEventsMs: generationRate,
                windowSizeSeconds: windowSize,
                slideSizeSeconds: slideSize,
                schemaType: 4, // Complex
                queryType: 3); // MultiMetric
        }

        // ═════════════════════════════════════════════════════════════════════
        // Helper Methods
        // ═════════════════════════════════════════════════════════════════════

        private static string GetSchemaTypeName(int schemaType)
        {
            return schemaType switch
            {
                1 => "Simple",
                2 => "Metrics",
                3 => "Sensors",
                4 => "Complex",
                _ => "Unknown"
            };
        }

        private static string GetQueryTypeName(int queryType)
        {
            return queryType switch
            {
                1 => "CountOnly",
                2 => "FieldAggregation",
                3 => "MultiMetric",
                _ => "Unknown"
            };
        }

        private static (KqlTableSchema schema, string kqlText) CreateSchemaByType(int schemaType)
        {
            string kqlText = schemaType switch
            {
                1 => @"
.create table Events (
    Timestamp: datetime,
    EventId: guid,
    EventType: string,
    Message: string
)",
                2 => @"
.create table Metrics (
    Timestamp: datetime,
    MetricName: string,
    Value: long,
    Unit: string,
    Source: string
)",
                3 => @"
.create table SensorData (
    Timestamp: datetime,
    SensorId: string,
    Temperature: long,
    Humidity: real,
    Pressure: real,
    Location: string
)",
                4 => @"
.create table ComplexEvents (
    Timestamp: datetime,
    EventId: guid,
    UserId: string,
    IsActive: bool,
    Value: long,
    Score: real,
    Duration: timespan,
    Tags: dynamic
)",
                _ => throw new ArgumentException($"Unknown schema type: {schemaType}")
            };

            var schema = KqlSchemaParser.Parse(kqlText);
            return (schema, kqlText);
        }

        private static KqlQueryConfig CreateConfigByType(
            int queryType,
            int schemaType,
            int windowSizeSeconds,
            int slideSizeSeconds)
        {
            var windowSize = TimeSpan.FromSeconds(windowSizeSeconds);
            var slideSize = TimeSpan.FromSeconds(slideSizeSeconds);

            // Determine aggregation field based on schema type
            string aggregationField = schemaType switch
            {
                2 => "Value",       // Metrics
                3 => "Temperature", // Sensors
                4 => "Value",       // Complex
                _ => null
            };

            return queryType switch
            {
                1 => KqlQueryConfig.CreateDefault(),
                2 => KqlQueryConfig.CreateFieldAggregation(aggregationField, windowSize, slideSize),
                3 => KqlQueryConfig.CreateMultiMetric(aggregationField, windowSize, slideSize),
                _ => KqlQueryConfig.CreateDefault()
            };
        }

        private static KqlDynamicRecord GenerateEventByType(
            KqlTableSchema schema,
            int schemaType,
            DateTime startTime,
            int index,
            Random random)
        {
            var timestamp = startTime.AddSeconds(index * 0.1);

            return schemaType switch
            {
                1 => new KqlDynamicRecordBuilder(schema)
                    .Set("Timestamp", timestamp)
                    .Set("EventId", Guid.NewGuid())
                    .Set("EventType", new[] { "Info", "Warning", "Error" }[random.Next(3)])
                    .Set("Message", $"Event message #{index}")
                    .Build(),

                2 => new KqlDynamicRecordBuilder(schema)
                    .Set("Timestamp", timestamp)
                    .Set("MetricName", new[] { "CPU", "Memory", "Disk", "Network" }[random.Next(4)])
                    .Set("Value", (long)random.Next(10, 100))
                    .Set("Unit", new[] { "percent", "MB", "GB", "Mbps" }[random.Next(4)])
                    .Set("Source", $"Server-{random.Next(1, 6)}")
                    .Build(),

                3 => new KqlDynamicRecordBuilder(schema)
                    .Set("Timestamp", timestamp)
                    .Set("SensorId", $"Sensor-{(char)('A' + random.Next(0, 5))}")
                    .Set("Temperature", (long)random.Next(15, 35))
                    .Set("Humidity", random.NextDouble() * 100)
                    .Set("Pressure", 980 + random.NextDouble() * 50)
                    .Set("Location", $"Building-{random.Next(1, 4)}")
                    .Build(),

                4 => CreateComplexEvent(schema, timestamp, index, random),

                _ => throw new ArgumentException($"Unknown schema type: {schemaType}")
            };
        }

        private static KqlDynamicRecord CreateComplexEvent(
            KqlTableSchema schema,
            DateTime timestamp,
            int index,
            Random random)
        {
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
        }

        // ─────────────────────────────────────────────────────────────────────
        // Event Generators (existing methods)
        // ─────────────────────────────────────────────────────────────────────

        private static List<StreamEvent<KqlDynamicRecord>> GenerateSimpleEvents(
            KqlTableSchema schema,
            DateTime startTime,
            int count)
        {
            var events = new List<StreamEvent<KqlDynamicRecord>>();
            var random = new Random(42);

            for (int i = 0; i < count; i++)
            {
                var timestamp = startTime.AddSeconds(i * 0.5);

                var record = new KqlDynamicRecordBuilder(schema)
                    .Set("Timestamp", timestamp)
                    .Set("EventId", Guid.NewGuid())
                    .Set("Message", $"Event message {i}")
                    .Build();

                var streamEvent = new StreamEvent<KqlDynamicRecord>(
                    timestamp.Ticks,
                    StreamEvent.InfinitySyncTime,
                    record);

                events.Add(streamEvent);
            }

            return events;
        }

        private static List<StreamEvent<KqlDynamicRecord>> GenerateMetricEvents(
            KqlTableSchema schema,
            DateTime startTime,
            int count)
        {
            var events = new List<StreamEvent<KqlDynamicRecord>>();
            var random = new Random(42);
            var metricNames = new[] { "CPU", "Memory", "Disk", "Network" };

            for (int i = 0; i < count; i++)
            {
                var timestamp = startTime.AddSeconds(i * 0.3);
                var value = random.Next(10, 100);

                var record = new KqlDynamicRecordBuilder(schema)
                    .Set("Timestamp", timestamp)
                    .Set("MetricName", metricNames[i % metricNames.Length])
                    .Set("Value", (long)value)
                    .Set("Source", $"Server-{random.Next(1, 4)}")
                    .Build();

                var streamEvent = new StreamEvent<KqlDynamicRecord>(
                    timestamp.Ticks,
                    StreamEvent.InfinitySyncTime,
                    record);

                events.Add(streamEvent);
            }

            return events;
        }

        private static List<StreamEvent<KqlDynamicRecord>> GenerateSensorEvents(
            KqlTableSchema schema,
            DateTime startTime,
            int count)
        {
            var events = new List<StreamEvent<KqlDynamicRecord>>();
            var random = new Random(42);
            var sensors = new[] { "Sensor-A", "Sensor-B", "Sensor-C" };
            var locations = new[] { "Building-1", "Building-2", "Building-3" };

            for (int i = 0; i < count; i++)
            {
                var timestamp = startTime.AddSeconds(i * 0.2);
                var temperature = random.Next(15, 35);
                var humidity = random.NextDouble() * 100;

                var record = new KqlDynamicRecordBuilder(schema)
                    .Set("Timestamp", timestamp)
                    .Set("SensorId", sensors[i % sensors.Length])
                    .Set("Temperature", (long)temperature)
                    .Set("Humidity", humidity)
                    .Set("Location", locations[random.Next(locations.Length)])
                    .Build();

                var streamEvent = new StreamEvent<KqlDynamicRecord>(
                    timestamp.Ticks,
                    StreamEvent.InfinitySyncTime,
                    record);

                events.Add(streamEvent);
            }

            return events;
        }

        private static List<StreamEvent<KqlDynamicRecord>> GenerateComplexEvents(
            KqlTableSchema schema,
            DateTime startTime,
            int count)
        {
            var events = new List<StreamEvent<KqlDynamicRecord>>();
            var random = new Random(42);

            for (int i = 0; i < count; i++)
            {
                var timestamp = startTime.AddSeconds(i * 0.4);
                var value = random.Next(100, 1000);
                var score = random.NextDouble() * 100;
                var duration = TimeSpan.FromSeconds(random.Next(1, 60));

                var tags = new Dictionary<string, object>
                {
                    { "Category", $"Cat-{random.Next(1, 4)}" },
                    { "Priority", random.Next(1, 6) },
                    { "Region", $"Region-{(char)('A' + random.Next(0, 3))}" }
                };

                var record = new KqlDynamicRecordBuilder(schema)
                    .Set("Timestamp", timestamp)
                    .Set("EventId", Guid.NewGuid())
                    .Set("UserId", $"User-{random.Next(1, 10)}")
                    .Set("IsActive", random.Next(2) == 0)
                    .Set("Value", (long)value)
                    .Set("Score", score)
                    .Set("Duration", duration)
                    .Set("Tags", tags)
                    .Build();

                var streamEvent = new StreamEvent<KqlDynamicRecord>(
                    timestamp.Ticks,
                    StreamEvent.InfinitySyncTime,
                    record);

                events.Add(streamEvent);
            }

            return events;
        }

        /// <summary>
        /// Demonstrate schema parsing and validation
        /// </summary>
        public static void DemonstrateSchemaFeatures()
        {
            Console.WriteLine("\n╔════════════════════════════════════════════════════════════════════╗");
            Console.WriteLine("║              KQL Schema Features Demonstration                     ║");
            Console.WriteLine("╚════════════════════════════════════════════════════════════════════╝\n");

            // Parse various schema formats
            var schemas = new[]
            {
                // Full table definition
                ".create table Events (Timestamp: datetime, Value: long)",

                // Datatable style
                "let schema = datatable(Id: guid, Name: string, Score: real)",

                // Simple column list
                "EventTime: datetime, EventType: string, Count: int",

                // Complex schema
                KqlSchemaParser.CreateSampleSchema("ComplexTable")
            };

            foreach (var schemaStr in schemas)
            {
                try
                {
                    Console.WriteLine($"Input: {schemaStr.Replace("\n", " ").Substring(0, Math.Min(60, schemaStr.Length))}...");
                    var schema = KqlSchemaParser.Parse(schemaStr);
                    Console.WriteLine($"✓ Parsed: {schema}");
                    Console.WriteLine();
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"✗ Error: {ex.Message}");
                    Console.WriteLine();
                }
            }
        }
    }
}
