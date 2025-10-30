# KQL Dynamic Record Serialization Fix

## Problem

When using `KqlDynamicRecord` with Trill's checkpointing functionality, a `SerializationException` was thrown:

```
System.Runtime.Serialization.SerializationException: Type 'EventHubReceiver.Kql.KqlDynamicRecord' is not supported.
```

This error occurred because `KqlDynamicRecord` contains:
1. A `Dictionary<string, object> data` field - which Trill's serializer cannot handle directly
2. A `KqlTableSchema schema` field - which contains complex types

## Root Cause

Trill's `ReflectionSchemaBuilder` recursively builds a schema for serialization. When it encounters `KqlDynamicRecord`, it tries to serialize its fields:

1. It encounters the `Dictionary<string, object>` field
2. Even with `ObjectDictionarySurrogate` configured, the surrogate is only checked at the top level
3. The recursive schema building fails before the surrogate can be applied
4. The error is thrown at line 103 in `ReflectionSchemaBuilder.CreateSchema()`

## Solution

Created a **combined surrogate** that handles both `KqlDynamicRecord` and its internal `Dictionary<string, object>`:

### 1. KqlDynamicRecordSurrogate

A surrogate class that:
- Serializes `KqlDynamicRecord` by writing:
  - Event counter
  - Schema table name
  - Data dictionary (using `ObjectDictionarySurrogate`)
  
- Deserializes by:
  - Reading the serialized data
  - Reconstructing a minimal schema from the data
  - Creating a new `KqlDynamicRecord` instance

**Key Features:**
- Delegates `Dictionary<string, object>` serialization to `ObjectDictionarySurrogate`
- Infers schema types from actual data values during deserialization
- Preserves the `EventCounter` across checkpoint/restore cycles

### 2. KqlSurrogate (Combined Surrogate)

A wrapper surrogate that provides methods for both types:
- `SerializeRecord` / `DeserializeRecord` - for `KqlDynamicRecord` types
- `SerializeDictionary` / `DeserializeDictionary` - for `Dictionary<string, object>` types

**IMPORTANT:** The surrogate methods must be defined on the same class that is passed to `QueryContainer`. This is because Trill's `SurrogateSerializer` creates an `Expression.Call` with the surrogate instance as the target:

```csharp
// From SurrogateSerializer.cs line 27
var surrogate = Expression.Constant(this.Surrogate);
return Expression.Call(surrogate, this.serialize, new[] { value, stream });
```

If `IsSupportedType` returns a method from a different class, you'll get:
```
ArgumentException: Method 'X' declared on type 'Y' cannot be called with instance of type 'Z'
```

### 3. Updated KqlEventProcessor

Changed the `QueryContainer` initialization from:
```csharp
queryContainer = new QueryContainer(new ObjectDictionarySurrogate());
```

To:
```csharp
queryContainer = new QueryContainer(new KqlSurrogate());
```

## Files Added

- `TrillSamples/src/EventHubReceiver/Kql/KqlDynamicRecordSurrogate.cs` - Contains:
  - `KqlDynamicRecordSurrogate` class (handles direct serialization)
  - `KqlSurrogate` class (combined surrogate with its own methods)

## Files Modified

- `TrillSamples/src/EventHubReceiver/Kql/KqlEventProcessor.cs` - Line 252:
  - Changed to use `KqlSurrogate` instead of `ObjectDictionarySurrogate`

## How It Works

1. **During Serialization:**
   ```
   KqlDynamicRecord
      ??> EventCounter (long) - serialized directly
      ??> Schema.TableName (string) - serialized directly  
      ??> data (Dictionary<string, object>) - handled by ObjectDictionarySurrogate
           ??> Each value serialized as JSON string
   ```

2. **During Deserialization:**
   ```
   Stream
      ??> Read EventCounter
      ??> Read TableName
      ??> Deserialize Dictionary<string, object>
      ??> Infer schema from data types
      ??> Reconstruct KqlDynamicRecord
   ```

## Surrogate Pattern Requirements

When implementing a custom surrogate for Trill, follow these rules:

### ? Correct Pattern
```csharp
public sealed class MySurrogate : ISurrogate
{
    public bool IsSupportedType(Type type, out MethodInfo serialize, out MethodInfo deserialize)
    {
        if (type == typeof(MyType))
        {
            // Return methods from THIS class
            serialize = typeof(MySurrogate).GetMethod(nameof(SerializeMyType));
            deserialize = typeof(MySurrogate).GetMethod(nameof(DeserializeMyType));
            return true;
        }
        
        serialize = null;
        deserialize = null;
        return false;
    }
    
    // Methods MUST be on this class
    public void SerializeMyType(MyType value, Stream stream) { ... }
    public MyType DeserializeMyType(Stream stream) { ... }
}
```

### ? Incorrect Pattern
```csharp
public sealed class MySurrogate : ISurrogate
{
    private readonly OtherSurrogate other = new OtherSurrogate();
    
    public bool IsSupportedType(Type type, out MethodInfo serialize, out MethodInfo deserialize)
    {
        // ? WRONG: Returning methods from a different class
        return other.IsSupportedType(type, out serialize, out deserialize);
    }
}
```

This will fail at runtime with:
```
ArgumentException: Method 'X' declared on type 'OtherSurrogate' 
cannot be called with instance of type 'MySurrogate'
```

### Delegation Pattern

If you need to delegate to another surrogate, wrap the calls:

```csharp
public sealed class CombinedSurrogate : ISurrogate
{
    private readonly SpecificSurrogate specific = new SpecificSurrogate();
    
    public bool IsSupportedType(Type type, out MethodInfo serialize, out MethodInfo deserialize)
    {
        if (/* condition */)
        {
            // Return OUR methods
            serialize = typeof(CombinedSurrogate).GetMethod(nameof(SerializeWrapper));
            deserialize = typeof(CombinedSurrogate).GetMethod(nameof(DeserializeWrapper));
            return true;
        }
        
        serialize = null;
        deserialize = null;
        return false;
    }
    
    // Wrapper methods that delegate
    public void SerializeWrapper(MyType value, Stream stream)
    {
        specific.Serialize(value, stream);
    }
    
    public MyType DeserializeWrapper(Stream stream)
    {
        return specific.Deserialize(stream);
    }
}
```

## Type Inference

During deserialization, the surrogate infers KQL data types from values:

| Value Type | Inferred KqlDataType |
|------------|---------------------|
| `bool` | `KqlDataType.Bool` |
| `DateTime` | `KqlDataType.DateTime` |
| `Guid` | `KqlDataType.Guid` |
| `int` | `KqlDataType.Int` |
| `long` | `KqlDataType.Long` |
| `double` / `float` | `KqlDataType.Real` |
| `decimal` | `KqlDataType.Decimal` |
| `string` | `KqlDataType.String` |
| `TimeSpan` | `KqlDataType.Timespan` |
| `Dictionary<string, object>` | `KqlDataType.Dynamic` |
| Everything else | `KqlDataType.Dynamic` |

## Testing

The solution enables:
1. ? Checkpointing of `KqlDynamicRecord` state
2. ? Restoration from checkpoints
3. ? Event counter preservation across restarts
4. ? All KQL data types supported
5. ? Complex nested objects via JSON serialization
6. ? Proper method binding in expression trees

## Limitations

1. **Schema Reconstruction:** During deserialization, the schema is inferred from data. Column metadata (descriptions, nullability constraints) is not preserved.

2. **Type Fidelity:** Complex objects are deserialized as `Dictionary<string, object>` or `List<object>`, not as their original types.

3. **Performance:** JSON serialization adds overhead compared to binary serialization.

## Usage Example

```csharp
// Define schema
var schema = KqlSchemaParser.Parse(@"
    .create table Metrics (
        Timestamp: datetime,
        MetricName: string,
        Value: long,
        Tags: dynamic
    )
");

// Create processor with KQL surrogate (automatically used)
var processor = new KqlEventProcessor(
    "partition-0",
    "checkpoints",
    schema,
    KqlQueryConfig.CreateMultiMetric("Value")
);

processor.Initialize();

// Process events - checkpointing works automatically
var record = new KqlDynamicRecordBuilder(schema)
    .Set("Timestamp", DateTime.UtcNow)
    .Set("MetricName", "CPU")
    .Set("Value", 85L)
    .Set("Tags", new Dictionary<string, object> { { "Host", "Server1" } })
    .Build();

processor.ProcessEvent(StreamEvent.CreateStart(0, record));

// Checkpoint automatically taken every 10 seconds
// Restore automatically happens on next Initialize()
```

## Benefits

1. **Seamless Integration:** Works with existing Trill checkpoint/restore mechanisms
2. **No Code Changes Required:** Existing `KqlDynamicRecord` usage remains unchanged
3. **Schema Flexibility:** Supports dynamic schemas with arbitrary column types
4. **Checkpoint Compatibility:** Event counters and state preserved across restarts
5. **Extensible:** Easy to add support for additional types in the surrogate
6. **Correct Expression Binding:** Methods are properly bound to surrogate instance

## Future Enhancements

1. **Schema Preservation:** Store full schema metadata in checkpoint for exact reconstruction
2. **Binary Serialization:** Custom binary format for better performance
3. **Type Registry:** Register custom type converters for domain-specific types
4. **Compression:** Add compression for large dictionaries or nested objects

## Troubleshooting

### Error: "Method 'X' declared on type 'Y' cannot be called with instance of type 'Z'"

**Cause:** The `IsSupportedType` method returned a `MethodInfo` from a different class than the surrogate instance.

**Solution:** Ensure all serialize/deserialize methods are defined on the surrogate class itself, not on a delegate class. Use wrapper methods if you need to delegate functionality.

### Error: "Type 'X' is not supported"

**Cause:** The surrogate's `IsSupportedType` returned `false` for the type, or no surrogate was configured.

**Solution:** 
1. Verify the surrogate is passed to `QueryContainer` constructor
2. Check that `IsSupportedType` correctly identifies your type
3. Ensure the type check logic matches your actual types

### Checkpoint Fails to Restore

**Cause:** Schema inference may fail if data types changed between checkpoint and restore.

**Solution:** Keep schemas consistent across checkpoint/restore cycles, or implement full schema serialization.
