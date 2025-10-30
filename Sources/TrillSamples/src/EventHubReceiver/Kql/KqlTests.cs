// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.StreamProcessing;

namespace EventHubReceiver.Kql
{
    /// <summary>
    /// Tests for KQL schema parsing and event processing
    /// </summary>
    public static class KqlTests
    {
        public static void RunAllTests()
        {
            Console.WriteLine("??????????????????????????????????????????????????????????????????????");
            Console.WriteLine("?                  KQL Implementation Tests                          ?");
            Console.WriteLine("??????????????????????????????????????????????????????????????????????\n");

#pragma warning disable SA1009 // Closing parenthesis must be spaced correctly
      var testResults = new List<(string Name, bool Passed, string Message)>();
#pragma warning restore SA1009 // Closing parenthesis must be spaced correctly

      // Schema parsing tests
      testResults.Add(TestSimpleSchemaParsing());
            testResults.Add(TestFullTableDefinition());
            testResults.Add(TestDatatableFormat());
            testResults.Add(TestAllDataTypes());
            testResults.Add(TestInvalidSchema());

            // Record tests
            testResults.Add(TestRecordCreation());
            testResults.Add(TestTypeConversion());
            testResults.Add(TestSchemaValidation());
            testResults.Add(TestRecordCloning());

            // Processor tests
            testResults.Add(TestCountQuery());
            testResults.Add(TestFieldAggregation());
            testResults.Add(TestMultiMetric());
            testResults.Add(TestCheckpointing());

            // Print results
            Console.WriteLine("\n" + new string('?', 72));
            Console.WriteLine("Test Results:");
            Console.WriteLine(new string('?', 72));

            int passed = 0, failed = 0;
            foreach (var result in testResults)
            {
                var status = result.Passed ? "? PASS" : "? FAIL";
                var color = result.Passed ? ConsoleColor.Green : ConsoleColor.Red;

                Console.ForegroundColor = color;
                Console.Write($"{status}");
                Console.ResetColor();
                Console.WriteLine($" {result.Name}");

                if (!result.Passed)
                {
                    Console.WriteLine($"     {result.Message}");
                    failed++;
                }
                else
                {
                    passed++;
                }
            }

            Console.WriteLine(new string('?', 72));
            Console.WriteLine($"Total: {testResults.Count} | Passed: {passed} | Failed: {failed}");
            Console.WriteLine(new string('?', 72) + "\n");
        }

        // ?????????????????????????????????????????????????????????????????????
        // Schema Parsing Tests
        // ?????????????????????????????????????????????????????????????????????

        private static(string, bool, string) TestSimpleSchemaParsing()
        {
            try
            {
                var schema = KqlSchemaParser.Parse("Column1: long, Column2: string");

                if (schema.Columns.Count != 2)
                    return("Simple Schema Parsing", false, $"Expected 2 columns, got {schema.Columns.Count}");

                if (schema.Columns[0].Name != "Column1" || schema.Columns[0].DataType != KqlDataType.Long)
                    return("Simple Schema Parsing", false, "Column1 type mismatch");

                if (schema.Columns[1].Name != "Column2" || schema.Columns[1].DataType != KqlDataType.String)
                    return("Simple Schema Parsing", false, "Column2 type mismatch");

                return("Simple Schema Parsing", true, string.Empty);
            }
            catch (Exception ex)
            {
                return("Simple Schema Parsing", false, ex.Message);
            }
        }

        private static(string, bool, string) TestFullTableDefinition()
        {
            try
            {
                var kql = ".create table MyTable (Timestamp: datetime, Value: long)";
                var schema = KqlSchemaParser.Parse(kql);

                if (schema.TableName != "MyTable")
                    return("Full Table Definition", false, $"Expected 'MyTable', got '{schema.TableName}'");

                if (schema.Columns.Count != 2)
                    return("Full Table Definition", false, $"Expected 2 columns, got {schema.Columns.Count}");

                return("Full Table Definition", true, string.Empty);
            }
            catch (Exception ex)
            {
                return("Full Table Definition", false, ex.Message);
            }
        }

        private static(string, bool, string) TestDatatableFormat()
        {
            try
            {
                var kql = "let schema = datatable(Id: guid, Name: string)";
                var schema = KqlSchemaParser.Parse(kql);

                if (schema.TableName != "schema")
                    return("Datatable Format", false, $"Expected 'schema', got '{schema.TableName}'");

                if (!schema.HasColumn("Id") || !schema.HasColumn("Name"))
                    return("Datatable Format", false, "Missing expected columns");

                return("Datatable Format", true, string.Empty);
            }
            catch (Exception ex)
            {
                return("Datatable Format", false, ex.Message);
            }
        }

        private static(string, bool, string) TestAllDataTypes()
        {
            try
            {
                var kql = @"
                    Bool: bool,
                    Date: datetime,
                    Dyn: dynamic,
                    Id: guid,
                    Int: int,
                    Long: long,
                    Real: real,
                    Str: string,
                    Time: timespan,
                    Dec: decimal";

                var schema = KqlSchemaParser.Parse(kql);

                if (schema.Columns.Count != 10)
                    return("All Data Types", false, $"Expected 10 columns, got {schema.Columns.Count}");

                var expectedTypes = new Dictionary<string, KqlDataType>
                {
                    { "Bool", KqlDataType.Bool },
                    { "Date", KqlDataType.DateTime },
                    { "Dyn", KqlDataType.Dynamic },
                    { "Id", KqlDataType.Guid },
                    { "Int", KqlDataType.Int },
                    { "Long", KqlDataType.Long },
                    { "Real", KqlDataType.Real },
                    { "Str", KqlDataType.String },
                    { "Time", KqlDataType.Timespan },
                    { "Dec", KqlDataType.Decimal }
                };

                foreach (var expected in expectedTypes)
                {
                    var column = schema.GetColumn(expected.Key);
                    if (column == null || column.DataType != expected.Value)
                        return("All Data Types", false, $"Type mismatch for {expected.Key}");
                }

                return("All Data Types", true, string.Empty);
            }
            catch (Exception ex)
            {
                return("All Data Types", false, ex.Message);
            }
        }

        private static(string, bool, string) TestInvalidSchema()
        {
            try
            {
                // Should throw exception for invalid type
                KqlSchemaParser.Parse("Column1: InvalidType");
                return("Invalid Schema Detection", false, "Should have thrown exception");
            }
            catch (ArgumentException)
            {
                return("Invalid Schema Detection", true, string.Empty);
            }
            catch (Exception ex)
            {
                return("Invalid Schema Detection", false, $"Wrong exception type: {ex.GetType().Name}");
            }
        }

        // ?????????????????????????????????????????????????????????????????????
        // Record Tests
        // ?????????????????????????????????????????????????????????????????????

        private static(string, bool, string) TestRecordCreation()
        {
            try
            {
                var schema = KqlSchemaParser.Parse("Name: string, Age: int, Score: real");

                var record = new KqlDynamicRecordBuilder(schema)
                    .Set("Name", "Alice")
                    .Set("Age", 30)
                    .Set("Score", 95.5)
                    .Build();

                if (record.GetValue<string>("Name") != "Alice")
                    return("Record Creation", false, "Name value mismatch");

                if (record.GetValue<int>("Age") != 30)
                    return("Record Creation", false, "Age value mismatch");

                if (Math.Abs(record.GetValue<double>("Score") - 95.5) > 0.001)
                    return("Record Creation", false, "Score value mismatch");

                return("Record Creation", true, string.Empty);
            }
            catch (Exception ex)
            {
                return("Record Creation", false, ex.Message);
            }
        }

        private static(string, bool, string) TestTypeConversion()
        {
            try
            {
                var schema = KqlSchemaParser.Parse("Value: long, Timestamp: datetime");
                var record = new KqlDynamicRecord(schema);

                // Test int to long conversion
                record.SetValue("Value", 42);
                if (record.GetValue<long>("Value") != 42L)
                    return("Type Conversion", false, "Int to long conversion failed");

                // Test string to DateTime conversion
                var dateStr = "2024-01-15T10:30:00Z";
                record.SetValue("Timestamp", dateStr);
                var dt = record.GetValue<DateTime>("Timestamp");
                if (dt.Year != 2024 || dt.Month != 1 || dt.Day != 15)
                    return("Type Conversion", false, "String to DateTime conversion failed");

                return("Type Conversion", true, string.Empty);
            }
            catch (Exception ex)
            {
                return("Type Conversion", false, ex.Message);
            }
        }

        private static(string, bool, string) TestSchemaValidation()
        {
            try
            {
                var schema = KqlSchemaParser.Parse("Required: string, Optional: int");
                var record = new KqlDynamicRecord(schema);

                // Try to set non-existent column
                try
                {
                    record.SetValue("NonExistent", "value");
                    return("Schema Validation", false, "Should reject non-existent column");
                }
                catch (ArgumentException)
                {
                    // Expected
                }

                // Verify HasColumn
                if (!schema.HasColumn("Required"))
                    return("Schema Validation", false, "HasColumn failed");

                return("Schema Validation", true, string.Empty);
            }
            catch (Exception ex)
            {
                return("Schema Validation", false, ex.Message);
            }
        }

        private static(string, bool, string) TestRecordCloning()
        {
            try
            {
                var schema = KqlSchemaParser.Parse("Value: long");
                var original = new KqlDynamicRecordBuilder(schema)
                    .Set("Value", 100L)
                    .SetEventCounter(42)
                    .Build();

                var clone = original.Clone();
                clone.SetValue("Value", 200L);

                if (original.GetValue<long>("Value") != 100L)
                    return("Record Cloning", false, "Original was modified");

                if (clone.GetValue<long>("Value") != 200L)
                    return("Record Cloning", false, "Clone not independent");

                if (clone.EventCounter != 42)
                    return("Record Cloning", false, "EventCounter not cloned");

                return("Record Cloning", true, string.Empty);
            }
            catch (Exception ex)
            {
                return("Record Cloning", false, ex.Message);
            }
        }

        // ?????????????????????????????????????????????????????????????????????
        // Processor Tests
        // ?????????????????????????????????????????????????????????????????????

        private static(string, bool, string) TestCountQuery()
        {
            try
            {
                var schema = KqlSchemaParser.Parse("Timestamp: datetime, Value: long");
                var checkpointDir = Path.Combine(Path.GetTempPath(), "KqlTests", Guid.NewGuid().ToString());
                var config = KqlQueryConfig.CreateDefault();

                using var processor = new KqlEventProcessor("test-partition", checkpointDir, schema, config);
                processor.Initialize();

                // Process some events
                for (int i = 0; i < 10; i++)
                {
                    var record = new KqlDynamicRecordBuilder(schema)
                        .Set("Timestamp", DateTime.UtcNow)
                        .Set("Value", (long)i)
                        .Build();

                    var evt = new StreamEvent<KqlDynamicRecord>(
                        DateTime.UtcNow.Ticks,
                        StreamEvent.InfinitySyncTime,
                        record);

                    processor.ProcessEvent(evt);
                }

                processor.Flush();

                // Cleanup
                if (Directory.Exists(checkpointDir))
                    Directory.Delete(checkpointDir, true);

                return("Count Query", true, string.Empty);
            }
            catch (Exception ex)
            {
                return("Count Query", false, ex.Message);
            }
        }

        private static(string, bool, string) TestFieldAggregation()
        {
            try
            {
                var schema = KqlSchemaParser.Parse("Timestamp: datetime, Value: long");
                var checkpointDir = Path.Combine(Path.GetTempPath(), "KqlTests", Guid.NewGuid().ToString());
                var config = KqlQueryConfig.CreateFieldAggregation("Value");

                using var processor = new KqlEventProcessor("test-partition", checkpointDir, schema, config);
                processor.Initialize();

                for (int i = 0; i < 5; i++)
                {
                    var record = new KqlDynamicRecordBuilder(schema)
                        .Set("Timestamp", DateTime.UtcNow)
                        .Set("Value", (long)(i * 10))
                        .Build();

                    var evt = new StreamEvent<KqlDynamicRecord>(
                        DateTime.UtcNow.Ticks,
                        StreamEvent.InfinitySyncTime,
                        record);

                    processor.ProcessEvent(evt);
                }

                processor.Flush();

                if (Directory.Exists(checkpointDir))
                    Directory.Delete(checkpointDir, true);

                return("Field Aggregation", true, string.Empty);
            }
            catch (Exception ex)
            {
                return("Field Aggregation", false, ex.Message);
            }
        }

        private static(string, bool, string) TestMultiMetric()
        {
            try
            {
                var schema = KqlSchemaParser.Parse("Timestamp: datetime, Temperature: long");
                var checkpointDir = Path.Combine(Path.GetTempPath(), "KqlTests", Guid.NewGuid().ToString());
                var config = KqlQueryConfig.CreateMultiMetric("Temperature");

                using var processor = new KqlEventProcessor("test-partition", checkpointDir, schema, config);
                processor.Initialize();

                var temps = new long[] { 20, 22, 19, 25, 21 };
                foreach (var temp in temps)
                {
                    var record = new KqlDynamicRecordBuilder(schema)
                        .Set("Timestamp", DateTime.UtcNow)
                        .Set("Temperature", temp)
                        .Build();

                    var evt = new StreamEvent<KqlDynamicRecord>(
                        DateTime.UtcNow.Ticks,
                        StreamEvent.InfinitySyncTime,
                        record);

                    processor.ProcessEvent(evt);
                }

                processor.Flush();

                if (Directory.Exists(checkpointDir))
                    Directory.Delete(checkpointDir, true);

                return("Multi-Metric Query", true, string.Empty);
            }
            catch (Exception ex)
            {
                return("Multi-Metric Query", false, ex.Message);
            }
        }

        private static(string, bool, string) TestCheckpointing()
        {
            var checkpointDir = Path.Combine(Path.GetTempPath(), "KqlTests", "Checkpoint-" + Guid.NewGuid().ToString());

            try
            {
                var schema = KqlSchemaParser.Parse("Timestamp: datetime, Value: long");
                var config = KqlQueryConfig.CreateDefault();

                // Create processor and process events
                using (var processor = new KqlEventProcessor("cp-test", checkpointDir, schema, config))
                {
                    processor.Initialize();

                    for (int i = 0; i < 5; i++)
                    {
                        var record = new KqlDynamicRecordBuilder(schema)
                            .Set("Timestamp", DateTime.UtcNow)
                            .Set("Value", (long)i)
                            .Build();

                        var evt = new StreamEvent<KqlDynamicRecord>(
                            DateTime.UtcNow.Ticks,
                            StreamEvent.InfinitySyncTime,
                            record);

                        processor.ProcessEvent(evt);
                    }

                    processor.Flush();
                    System.Threading.Thread.Sleep(11000); // Wait for checkpoint
                }

                // Verify checkpoint files exist
                var checkpointFiles = Directory.GetFiles(checkpointDir, "*.checkpoint");
                var metadataFiles = Directory.GetFiles(checkpointDir, "*.metadata");

                if (checkpointFiles.Length == 0)
                    return("Checkpointing", false, "No checkpoint files created");

                if (metadataFiles.Length == 0)
                    return("Checkpointing", false, "No metadata files created");

                // Try to restore
                using (var processor = new KqlEventProcessor("cp-test", checkpointDir, schema, config))
                {
                    processor.Initialize(); // Should restore from checkpoint
                }

                if (Directory.Exists(checkpointDir))
                    Directory.Delete(checkpointDir, true);

                return("Checkpointing", true, string.Empty);
            }
            catch (Exception ex)
            {
                if (Directory.Exists(checkpointDir))
                {
                    try
                    {
                        Directory.Delete(checkpointDir, true);
                    }
                    catch
                    {
                    }
                }

                return("Checkpointing", false, ex.Message);
            }
        }
    }
}
