// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
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

            // Example 4: Complex schema with multiple types
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
        /// Example 4: Complex schema with multiple data types
        /// </summary>
        private static void RunComplexSchemaExample()
        {
            Console.WriteLine("Example 4: Complex Schema with Multiple Types");
            Console.WriteLine("──────────────────────────────────────────────");

            var kqlSchema = @"
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

            Console.WriteLine($"KQL Schema:\n{kqlSchema}\n");

            var schema = KqlSchemaParser.Parse(kqlSchema);

            // Display schema details
            Console.WriteLine("Schema Details:");
            foreach (var column in schema.Columns)
            {
                Console.WriteLine($"  {column.Name, -15} : {column.DataType, -10} (CLR: {column.ClrType.Name})");
            }

            Console.WriteLine();

            var checkpointDir = Path.Combine(Path.GetTempPath(), "KqlCheckpoints", "Complex");
            var config = KqlQueryConfig.CreateMultiMetric("Value");

            using var processor = new KqlEventProcessor("partition-0", checkpointDir, schema, config);
            processor.Initialize();

            var startTime = DateTime.UtcNow;
            var events = GenerateComplexEvents(schema, startTime, 25);

            Console.WriteLine($"Processing {events.Count} complex events...\n");

            foreach (var evt in events)
            {
                processor.ProcessEvent(evt);
            }

            processor.Flush();
            Thread.Sleep(2000);
        }

        // ─────────────────────────────────────────────────────────────────────
        // Event Generators
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
