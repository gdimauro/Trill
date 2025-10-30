# KQL Schema-Based Event Processing for Trill

## Overview

This implementation provides **Kusto Query Language (KQL)** schema support for Trill stream processing, enabling dynamic schema definition and strongly-typed event processing with the same checkpointing capabilities as the `LocalEventProcessor`.

## Key Features

- **KQL Schema Parsing**: Define data structures using familiar KQL table syntax
- **Dynamic Type Mapping**: Automatic conversion between KQL data types and .NET CLR types
- **Strongly-Typed Access**: Type-safe field access with runtime validation
- **Checkpoint Support**: Full state persistence and restoration across restarts
- **Multiple Aggregation Modes**: Count, field aggregation, and multi-metric analysis
- **Dictionary-Based Serialization**: Compatible with Trill's `ObjectDictionarySurrogate`

## Architecture

### Components

1. **KqlSchemaParser** - Parses KQL table definitions into strongly-typed schemas
2. **KqlTableSchema** - Represents the parsed schema with column metadata
3. **KqlDynamicRecord** - Schema-aware record with type-safe field access
4. **KqlEventProcessor** - Event processor with checkpointing (similar to `LocalEventProcessor`)
5. **KqlQueryConfig** - Configuration for different query types

### Supported KQL Data Types

| KQL Type   | CLR Type                    | Example                      |
|------------|-----------------------------|------------------------------|
| `bool`     | `bool`                      | `true`, `false`              |
| `datetime` | `DateTime`                  | `2024-01-15T10:30:00Z`       |
| `dynamic`  | `Dictionary<string,object>` | `{"key": "value"}`           |
| `guid`     | `Guid`                      | `550e8400-e29b-41d4-a716-...`|
| `int`      | `int`                       | `42`                         |
| `long`     | `long`                      | `1234567890`                 |
| `real`     | `double`                    | `3.14159`                    |
| `string`   | `string`                    | `"Hello, World!"`            |
| `timespan` | `TimeSpan`                  | `00:05:30`                   |
| `decimal`  | `decimal`                   | `123.45`                     |

## Usage Examples

### Example 1: Simple Event Counting

```csharp
// Define schema using KQL syntax
var kqlSchema = @"
.create table SimpleEvents (
    Timestamp: datetime,
    EventId: guid,
    Message: string
)";

// Parse the schema
var schema = KqlSchemaParser.Parse(kqlSchema);

// Create processor with count-only query
var config = KqlQueryConfig.CreateDefault();
using var processor = new KqlEventProcessor(
    partitionId: "partition-0",
    checkpointDirectory: @"C:\Checkpoints",
    schema: schema,
    queryConfig: config
);

processor.Initialize();

// Create and process events
var record = new KqlDynamicRecordBuilder(schema)
    .Set("Timestamp", DateTime.UtcNow)
    .Set("EventId", Guid.NewGuid())
    .Set("Message", "Hello, Trill!")
    .Build();

var streamEvent = new StreamEvent<KqlDynamicRecord>(
    DateTime.UtcNow.Ticks,
    StreamEvent.InfinitySyncTime,
    record
);

processor.ProcessEvent(streamEvent);
processor.Flush();
```

### Example 2: Field Aggregation

```csharp
var kqlSchema = @"
.create table MetricEvents (
    Timestamp: datetime,
    MetricName: string,
    Value: long,
    Source: string
)";

var schema = KqlSchemaParser.Parse(kqlSchema);

// Aggregate on 'Value' field
var config = KqlQueryConfig.CreateFieldAggregation("Value");
using var processor = new KqlEventProcessor("partition-0", @"C:\Checkpoints", schema, config);
processor.Initialize();

// Process metric events
var record = new KqlDynamicRecordBuilder(schema)
    .Set("Timestamp", DateTime.UtcNow)
    .Set("MetricName", "CPU")
    .Set("Value", 75L)
    .Set("Source", "Server-1")
    .Build();

processor.ProcessEvent(new StreamEvent<KqlDynamicRecord>(
    DateTime.UtcNow.Ticks,
    StreamEvent.InfinitySyncTime,
    record
));
```

### Example 3: Multi-Metric Analysis

```csharp
var kqlSchema = @"
.create table SensorData (
    Timestamp: datetime,
    SensorId: string,
    Temperature: long,
    Humidity: real,
    Location: string
)";

var schema = KqlSchemaParser.Parse(kqlSchema);

// Multi-metric aggregation with custom window
var config = KqlQueryConfig.CreateMultiMetric(
    fieldName: "Temperature",
    windowSize: TimeSpan.FromSeconds(10),
    slideSize: TimeSpan.FromSeconds(2)
);

using var processor = new KqlEventProcessor("partition-0", @"C:\Checkpoints", schema, config);
processor.Initialize();

// Process sensor readings
var record = new KqlDynamicRecordBuilder(schema)
    .Set("Timestamp", DateTime.UtcNow)
    .Set("SensorId", "Sensor-A")
    .Set("Temperature", 22L)
    .Set("Humidity", 45.5)
    .Set("Location", "Building-1")
    .Build();

processor.ProcessEvent(new StreamEvent<KqlDynamicRecord>(
    DateTime.UtcNow.Ticks,
    StreamEvent.InfinitySyncTime,
    record
));
```

### Example 4: Complex Schema with Dynamic Fields

```csharp
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

var schema = KqlSchemaParser.Parse(kqlSchema);

var tags = new Dictionary<string, object>
{
    { "Category", "Analytics" },
    { "Priority", 3 },
    { "Region", "US-West" }
};

var record = new KqlDynamicRecordBuilder(schema)
    .Set("Timestamp", DateTime.UtcNow)
    .Set("EventId", Guid.NewGuid())
    .Set("UserId", "user@example.com")
    .Set("IsActive", true)
    .Set("Value", 1000L)
    .Set("Score", 95.5)
    .Set("Duration", TimeSpan.FromMinutes(5))
    .Set("Tags", tags)
    .Build();

// Access fields with type safety
var userId = record.GetValue<string>("UserId");
var isActive = record.GetValue<bool>("IsActive");
var score = record.GetValue<double>("Score");
var metadata = record.GetValue<Dictionary<string, object>>("Tags");
```

## KQL Schema Format Support

The parser supports multiple KQL schema formats:

### Full Table Definition
```kql
.create table MyTable (
    Column1: type,
    Column2: type
)
```

### Datatable Style
```kql
let schema = datatable(Column1: type, Column2: type)
```

### Simple Column List
```kql
Column1: type, Column2: type
```

## Query Types

### 1. Count Only
Simple event counting in windows:
```csharp
var config = KqlQueryConfig.CreateDefault();
// Output: Count, EventCounter
```

### 2. Field Aggregation
Aggregate statistics on a specific field:
```csharp
var config = KqlQueryConfig.CreateFieldAggregation("Value");
// Output: Count, Sum, Average, EventCounter
```

### 3. Multi-Metric
Comprehensive statistics including min/max:
```csharp
var config = KqlQueryConfig.CreateMultiMetric("Temperature");
// Output: Count, Sum, Average, Min, Max, EventCounter
```

## Checkpointing

The `KqlEventProcessor` provides the same checkpointing capabilities as `LocalEventProcessor`:

- **Automatic Checkpoints**: Taken every 10 seconds
- **State Persistence**: Query state and event counters saved to disk
- **Restoration**: Automatic recovery from the latest checkpoint
- **Metadata Tracking**: Event counter and sequence number preserved

### Checkpoint Files

```
C:\Checkpoints\
??? partition-0-100.checkpoint     # Query state
??? partition-0-100.metadata       # Event counter:sequence
??? partition-0-200.checkpoint
??? partition-0-200.metadata
```

## Type Conversion and Validation

### Automatic Type Conversion

The system automatically converts between compatible types:

```csharp
// String to DateTime
record.SetValue("Timestamp", "2024-01-15T10:30:00Z");

// Numeric conversions
record.SetValue("Value", 42);      // int -> long
record.SetValue("Score", 95);      // int -> double

// String to Guid
record.SetValue("EventId", "550e8400-e29b-41d4-a716-446655440000");
```

### Schema Validation

```csharp
var record = new KqlDynamicRecord(schema);

// Validate required fields
if (!record.IsValid())
{
    var missing = record.GetMissingColumns();
    Console.WriteLine($"Missing: {string.Join(", ", missing)}");
}

// Check individual fields
if (record.HasValue("UserId"))
{
    var userId = record.GetValue<string>("UserId");
}
```

## Performance Characteristics

- **Memory Efficient**: Dictionary-based storage with lazy field access
- **Serialization**: Compatible with Trill's `ObjectDictionarySurrogate`
- **Type Safety**: Runtime type checking with graceful fallbacks
- **Checkpoint Size**: Comparable to `LocalEventProcessor` and `DynamicEventProcessor`

## Comparison with Other Processors

| Feature                    | LocalEventProcessor | DynamicEventProcessor | KqlEventProcessor |
|----------------------------|---------------------|----------------------|-------------------|
| Schema Definition          | Hardcoded structs   | FlexiblePayload      | KQL syntax        |
| Type Safety               | Compile-time        | Runtime              | Runtime           |
| Checkpointing             | ?                   | ?                    | ?                 |
| Dynamic Fields            | ?                   | ?                    | ?                 |
| Schema Validation         | ?                   | Basic                | Full              |
| KQL Compatibility         | ?                   | ?                    | ?                 |
| Field-Level Aggregation   | ?                   | ?                    | ?                 |

## Best Practices

1. **Schema Design**
   - Use appropriate data types (e.g., `long` for large integers)
   - Mark optional fields explicitly in documentation
   - Use `dynamic` for truly flexible metadata

2. **Performance**
   - Minimize dictionary lookups by caching field values
   - Use batch processing with `ProcessEvents()` when possible
   - Set appropriate checkpoint intervals

3. **Error Handling**
   - Validate schema before creating processor
   - Handle type conversion errors gracefully
   - Check for missing required fields

4. **Testing**
   - Verify checkpoint restoration logic
   - Test with various data types
   - Validate schema parsing with edge cases

## Running the Sample

```csharp
// Run all examples
KqlProcessorSample.Run();

// Or run specific examples
KqlProcessorSample.DemonstrateSchemaFeatures();
```

## Output Example

```
??????????????????????????????????????????????????????????????????????
? [partition-0] KQL Window #1
? Schema: SensorData
? Time: 14:30:00.000 - 14:30:03.000
??????????????????????????????????????????????????????????????????????
? Query Type        : MultiMetric(Temperature)
? Count             :              15
? Event Counter     :              15
? Aggregated Field  : Temperature
? Sum               :             345
? Average           :           23.00
? Minimum           :              18
? Maximum           :              28
? Range             :              10
?
? === Historical Statistics ===
? Total Windows     :               1
? Total Events      :              15
? Cumulative Sum    :             345
? Cumulative Avg    :           23.00
? Events/Second     :            5.00
??????????????????????????????????????????????????????????????????????
```

## Integration with Existing Code

The KQL implementation follows the same pattern as `LocalEventProcessor`:

```csharp
// LocalEventProcessor pattern
var localProcessor = new LocalEventProcessor("partition-0", checkpointDir);
localProcessor.Initialize();
localProcessor.ProcessEvent(longEvent);

// KqlEventProcessor pattern - same structure!
var kqlProcessor = new KqlEventProcessor("partition-0", checkpointDir, schema, config);
kqlProcessor.Initialize();
kqlProcessor.ProcessEvent(kqlEvent);
```

## Advanced Features

### Custom Window Configurations

```csharp
var config = new KqlQueryConfig
{
    QueryType = KqlQueryType.MultiMetric,
    WindowSize = TimeSpan.FromMinutes(1),
    SlideSize = TimeSpan.FromSeconds(10),
    AggregationField = "ResponseTime"
};
```

### Record Cloning

```csharp
var original = CreateRecord();
var clone = original.Clone();
clone.SetValue("Value", 999L);
// original is unchanged
```

### Schema Introspection

```csharp
foreach (var column in schema.Columns)
{
    Console.WriteLine($"{column.Name}: {column.DataType} ({column.ClrType.Name})");
}

if (schema.HasColumn("UserId"))
{
    var column = schema.GetColumn("UserId");
    Console.WriteLine($"Found: {column}");
}
```

## Troubleshooting

### Common Issues

1. **"Column not found in schema"**
   - Verify column name matches schema definition exactly
   - Check for typos in column names

2. **"Cannot convert value to type"**
   - Ensure value is compatible with column type
   - Use explicit type conversion when needed

3. **"Aggregation field must be numeric"**
   - Verify aggregation field is `int`, `long`, `real`, or `decimal`
   - Check schema definition

4. **Checkpoint restoration fails**
   - Ensure schema hasn't changed between runs
   - Delete old checkpoints if schema changed
   - Check checkpoint directory permissions

## Future Enhancements

- Support for nullable types in schema
- Complex nested dynamic structures
- Multi-field aggregations
- Custom aggregation functions
- Schema versioning and migration
- KQL query compilation (beyond schema)

## See Also

- `LocalEventProcessor.cs` - Similar checkpointing pattern with fixed schema
- `DynamicEventProcessor.cs` - Flexible payload without KQL
- `DICTIONARY_OBJECT_SERIALIZATION.md` - Serialization details
- `COMPLEX_QUERY_GUIDE.md` - Advanced query patterns
