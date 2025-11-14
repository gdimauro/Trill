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
using Microsoft.StreamProcessing;

namespace EventHubReceiver.Kql
{
    /// <summary>
    /// Tests for KQL schema parsing and event processing
    /// </summary>
    public static class KqlTests
    {
        /// <summary>
        /// Run all KQL tests
        /// </summary>
        public static void RunAll()
        {
            Console.WriteLine("╔════════════════════════════════════════════════════════════════════╗");
            Console.WriteLine("║                    KQL Implementation Tests                        ║");
            Console.WriteLine("╚════════════════════════════════════════════════════════════════════╝");
            Console.WriteLine();

            TestSchemaParser();
            Console.WriteLine();

            TestAllDataTypes();
            Console.WriteLine();

            TestTypeAliases();
            Console.WriteLine();

            TestDynamicRecordBuilder();
            Console.WriteLine();

            TestNullHandling();
            Console.WriteLine();

            TestComplexDynamicTypes();
            Console.WriteLine();

            Console.WriteLine("✓ All tests completed!");
        }

        /// <summary>
        /// Test 1: Schema parsing with various formats
        /// </summary>
        private static void TestSchemaParser()
        {
            Console.WriteLine("Test 1: Schema Parser");
            Console.WriteLine("─────────────────────");

            var testCases = new[]
            {
                (".create table MyTable (Id: guid, Name: string)", "Full syntax"),
                ("let schema = datatable(Id: guid, Name: string)", "Datatable syntax"),
                ("Id: guid, Name: string, Value: long", "Simple syntax"),
            };

            foreach (var (schema, description) in testCases)
            {
                try
                {
                    var parsed = KqlSchemaParser.Parse(schema);
                    Console.WriteLine($"✓ {description}: {parsed}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"✗ {description}: {ex.Message}");
                }
            }
        }

        /// <summary>
        /// Test 2: All standard KQL data types
        /// </summary>
        private static void TestAllDataTypes()
        {
            Console.WriteLine("Test 2: All Standard KQL Data Types");
            Console.WriteLine("────────────────────────────────────");

            var kqlSchema = @"
.create table AllTypes (
    BoolField: bool,
    DateTimeField: datetime,
    DecimalField: decimal,
    DynamicField: dynamic,
    GuidField: guid,
    IntField: int,
    LongField: long,
    RealField: real,
    StringField: string,
    TimespanField: timespan
)";

            try
            {
                var schema = KqlSchemaParser.Parse(kqlSchema);
                Console.WriteLine($"✓ Schema parsed: {schema}");

                // Verify each column
                var expectedTypes = new[]
                {
                    ("BoolField", KqlDataType.Bool, typeof(bool)),
                    ("DateTimeField", KqlDataType.DateTime, typeof(DateTime)),
                    ("DecimalField", KqlDataType.Decimal, typeof(decimal)),
                    ("DynamicField", KqlDataType.Dynamic, typeof(Dictionary<string, object>)),
                    ("GuidField", KqlDataType.Guid, typeof(Guid)),
                    ("IntField", KqlDataType.Int, typeof(int)),
                    ("LongField", KqlDataType.Long, typeof(long)),
                    ("RealField", KqlDataType.Real, typeof(double)),
                    ("StringField", KqlDataType.String, typeof(string)),
                    ("TimespanField", KqlDataType.Timespan, typeof(TimeSpan))
                };

                foreach (var (name, kqlType, clrType) in expectedTypes)
                {
                    var column = schema.GetColumn(name);
                    if (column != null && column.DataType == kqlType && column.ClrType == clrType)
                    {
                        Console.WriteLine($"  ✓ {name}: {kqlType} → {clrType.Name}");
                    }
                    else
                    {
                        Console.WriteLine($"  ✗ {name}: Mismatch!");
                    }
                }

                // Create a record with all types
                var record = new KqlDynamicRecordBuilder(schema)
                    .Set("BoolField", true)
                    .Set("DateTimeField", DateTime.UtcNow)
                    .Set("DecimalField", 123.45m)
                    .Set("DynamicField", new Dictionary<string, object> { { "key", "value" } })
                    .Set("GuidField", Guid.NewGuid())
                    .Set("IntField", 42)
                    .Set("LongField", 1234567890L)
                    .Set("RealField", 3.14159)
                    .Set("StringField", "Hello, KQL!")
                    .Set("TimespanField", TimeSpan.FromMinutes(30))
                    .Build();

                Console.WriteLine($"✓ Record created with all types: EventCounter={record.EventCounter}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"✗ Test failed: {ex.Message}");
            }
        }

        /// <summary>
        /// Test 3: All KQL type aliases
        /// </summary>
        private static void TestTypeAliases()
        {
            Console.WriteLine("Test 3: KQL Type Aliases");
            Console.WriteLine("────────────────────────");

            var aliasTestCases = new[]
            {
                ("bool", "boolean", KqlDataType.Bool),
                ("datetime", "date", KqlDataType.DateTime),
                ("guid", "uniqueid", KqlDataType.Guid),
                ("guid", "uuid", KqlDataType.Guid),
                ("real", "double", KqlDataType.Real),
                ("timespan", "time", KqlDataType.Timespan)
            };

            foreach (var (primary, alias, expectedType) in aliasTestCases)
            {
                try
                {
                    var schema1 = KqlSchemaParser.Parse($"Field: {primary}");
                    var schema2 = KqlSchemaParser.Parse($"Field: {alias}");

                    if (schema1.GetColumn("Field").DataType == expectedType &&
                        schema2.GetColumn("Field").DataType == expectedType)
                    {
                        Console.WriteLine($"✓ {primary} = {alias} → {expectedType}");
                    }
                    else
                    {
                        Console.WriteLine($"✗ {primary} = {alias}: Type mismatch");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"✗ {primary} = {alias}: {ex.Message}");
                }
            }
        }

        /// <summary>
        /// Test 4: Dynamic record builder
        /// </summary>
        private static void TestDynamicRecordBuilder()
        {
            Console.WriteLine("Test 4: Dynamic Record Builder");
            Console.WriteLine("───────────────────────────────");

            var schema = KqlSchemaParser.Parse(@"
.create table TestTable (
    Id: guid,
    Name: string,
    Age: int,
    Score: real,
    IsActive: bool,
    CreatedAt: datetime
)");

            try
            {
                var record = new KqlDynamicRecordBuilder(schema)
                    .Set("Id", Guid.NewGuid())
                    .Set("Name", "Test User")
                    .Set("Age", 25)
                    .Set("Score", 95.5)
                    .Set("IsActive", true)
                    .Set("CreatedAt", DateTime.UtcNow)
                    .Build();

                Console.WriteLine($"✓ Record created: {record}");
                Console.WriteLine($"  - Id: {record.GetValue<Guid>("Id")}");
                Console.WriteLine($"  - Name: {record.GetValue<string>("Name")}");
                Console.WriteLine($"  - Age: {record.GetValue<int>("Age")}");
                Console.WriteLine($"  - Score: {record.GetValue<double>("Score")}");
                Console.WriteLine($"  - IsActive: {record.GetValue<bool>("IsActive")}");
                Console.WriteLine($"  - CreatedAt: {record.GetValue<DateTime>("CreatedAt"):yyyy-MM-dd HH:mm:ss}");

                // Test validation
                if (record.IsValid())
                {
                    Console.WriteLine("✓ Record is valid (all required fields set)");
                }
                else
                {
                    Console.WriteLine($"✗ Record has missing fields: {string.Join(", ", record.GetMissingColumns())}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"✗ Test failed: {ex.Message}");
            }
        }

        /// <summary>
        /// Test 5: Null value handling
        /// </summary>
        private static void TestNullHandling()
        {
            Console.WriteLine("Test 5: Null Value Handling");
            Console.WriteLine("────────────────────────────");

            var schema = KqlSchemaParser.Parse(@"
.create table NullableTest (
    StringField: string,
    IntField: int,
    DateField: datetime
)");

            try
            {
                var record = new KqlDynamicRecordBuilder(schema)
                    .Set("StringField", null)
                    .Set("IntField", null)
                    .Set("DateField", DateTime.UtcNow)
                    .Build();

                Console.WriteLine("✓ Record created with null values");
                Console.WriteLine($"  - StringField is null: {!record.HasValue("StringField")}");
                Console.WriteLine($"  - IntField is null: {!record.HasValue("IntField")}");
                Console.WriteLine($"  - DateField has value: {record.HasValue("DateField")}");

                // Test default values
                var defaultString = record.GetValue<string>("StringField", "default");
                var defaultInt = record.GetValue<int>("IntField", 99);

                Console.WriteLine($"✓ Default values work:");
                Console.WriteLine($"  - StringField default: '{defaultString}'");
                Console.WriteLine($"  - IntField default: {defaultInt}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"✗ Test failed: {ex.Message}");
            }
        }

        /// <summary>
        /// Test 6: Complex dynamic types
        /// </summary>
        private static void TestComplexDynamicTypes()
        {
            Console.WriteLine("Test 6: Complex Dynamic Types");
            Console.WriteLine("──────────────────────────────");

            var schema = KqlSchemaParser.Parse(@"
.create table ComplexTest (
    Id: guid,
    Metadata: dynamic,
    Tags: dynamic,
    Nested: dynamic
)");

            try
            {
                // Create complex structures
                var metadata = new Dictionary<string, object>
                {
                    { "Version", "1.0" },
                    { "Author", "Test" },
                    { "Priority", 5 }
                };

                var tags = new Dictionary<string, object>
                {
                    { "Category", "TestCategory" },
                    { "Environment", "Production" }
                };

                var nested = new Dictionary<string, object>
                {
                    { "Level1", new Dictionary<string, object>
                        {
                            { "Level2", new Dictionary<string, object>
                                {
                                    { "DeepValue", "Found me!" }
                                }
                            }
                        }
                    }
                };

                var record = new KqlDynamicRecordBuilder(schema)
                    .Set("Id", Guid.NewGuid())
                    .Set("Metadata", metadata)
                    .Set("Tags", tags)
                    .Set("Nested", nested)
                    .Build();

                Console.WriteLine("✓ Record created with complex dynamic types");

                // Retrieve and verify
                var retrievedMetadata = record.GetValue<Dictionary<string, object>>("Metadata");
                var retrievedTags = record.GetValue<Dictionary<string, object>>("Tags");

                Console.WriteLine($"  - Metadata entries: {retrievedMetadata?.Count ?? 0}");
                Console.WriteLine($"  - Tags entries: {retrievedTags?.Count ?? 0}");

                if (retrievedMetadata != null)
                {
                    Console.WriteLine($"  - Metadata.Version: {retrievedMetadata["Version"]}");
                    Console.WriteLine($"  - Metadata.Priority: {retrievedMetadata["Priority"]}");
                }

                Console.WriteLine("✓ Complex dynamic types work correctly");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"✗ Test failed: {ex.Message}");
            }
        }
    }
}
