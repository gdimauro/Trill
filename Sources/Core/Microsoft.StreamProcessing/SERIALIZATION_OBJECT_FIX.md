# Serialization Fix: Handling Dictionary<string, object> and Object Types

## Problem

When trying to serialize types containing `Dictionary<string, object>` or `object[]`, Trill's serializer throws:

```
System.Runtime.Serialization.SerializationException:
Type 'System.Object' is not supported by the resolver.
```

### Root Cause

Trill's serializer explicitly rejects `System.Object` as an unsupported type because:
1. `object` is too generic - cannot determine actual type at serialization time
2. Polymorphic serialization would require runtime type information
3. Type safety cannot be guaranteed

### Where This Occurs

```csharp
// ? These patterns trigger the error:

public record MyRecord
{
    public Dictionary<string, object> Tags { get; init; }  // object value type
}

public class MyClass
{
    public object SomeField;  // Direct object field
    public object[] Items;    // Array of objects
}
```

## Solution Implemented

### Updated `TypeExtensions.ResolveMembers()`

Added a new helper method `IsUnsupportedForSerialization()` that performs **deep type checking**:

```csharp
/// <summary>
/// Checks if a type or its collection element types are unsupported for serialization.
/// This is more comprehensive than IsUnsupported() as it also checks collection element types.
/// </summary>
private static bool IsUnsupportedForSerialization(Type type)
{
    // Direct check
    if (type.IsUnsupported()) return true;

    // Check generic types (Dictionary<K,V>, List<T>, etc.)
    if (type.GetTypeInfo().IsGenericType)
    {
        var genericArgs = type.GetGenericArguments();
        
        // For Dictionary<K,V>, List<T>, etc., check if any generic argument is unsupported
        foreach (var arg in genericArgs)
        {
            if (arg == typeof(object)) return true; // Specifically reject object
            if (arg.IsUnsupported()) return true;
        }
    }

    // Check array element type
    if (type.IsArray)
    {
        var elementType = type.GetElementType();
        if (elementType == typeof(object)) return true;
        if (elementType.IsUnsupported()) return true;
    }

    return false;
}
```

### What Gets Filtered Out

Now `ResolveMembers()` will **skip** these problematic properties during serialization:

```csharp
// ? Filtered out (not serialized):
public Dictionary<string, object> DynamicData { get; init; }
public List<object> Items { get; init; }
public object[] Array { get; init; }
public object Value { get; init; }

// ? Still serialized (supported types):
public Dictionary<string, string> Tags { get; init; }
public List<int> Numbers { get; init; }
public string[] Names { get; init; }
public int Value { get; init; }
```

## Impact on Sample Types

### Before Fix

```csharp
public record Transaction
{
    public Guid TransactionId { get; init; }
    public decimal Amount { get; init; }
    public MerchantInfo Merchant { get; init; }  // ? Serialized
    public Dictionary<string, object> Metadata { get; init; }  // ? ERROR!
}
```

**Result:** Serialization fails with "Type 'System.Object' is not supported"

### After Fix

```csharp
public record Transaction
{
    public Guid TransactionId { get; init; }  // ? Serialized
    public decimal Amount { get; init; }      // ? Serialized
    public MerchantInfo Merchant { get; init; }  // ? Serialized (if all its properties are supported)
    public Dictionary<string, object> Metadata { get; init; }  // ?? SKIPPED (not serialized, but no error)
}
```

**Result:** Partial serialization succeeds. `Metadata` is not checkpointed, but other fields are.

## Workarounds for Dynamic Data

If you need to serialize dynamic data, use these patterns:

### Workaround 1: Use Surrogate Pattern

```csharp
// Define a serializable surrogate
[DataContract]
public class DynamicDataSurrogate
{
    [DataMember]
    public string JsonData { get; set; }  // Serialize as JSON string
}

// Use surrogate in your type
public record MyRecord
{
    public Dictionary<string, string> Tags { get; init; }  // ? Supported!
    
    // Convert Dictionary<string, object> to JSON
    public string MetadataJson => JsonSerializer.Serialize(Metadata);
}
```

### Workaround 2: Use Strongly-Typed Alternatives

```csharp
// ? Avoid this:
public Dictionary<string, object> Metadata { get; init; }

// ? Use this instead:
public Dictionary<string, string> StringMetadata { get; init; }
public Dictionary<string, int> NumericMetadata { get; init; }
public Dictionary<string, MetadataValue> TypedMetadata { get; init; }

// Where MetadataValue is:
[DataContract]
public class MetadataValue
{
    [DataMember] public string StringValue { get; set; }
    [DataMember] public int? IntValue { get; set; }
    [DataMember] public double? DoubleValue { get; set; }
}
```

### Workaround 3: Mark as Non-Serialized

```csharp
public class MyClass
{
    public string Name { get; set; }  // ? Serialized
    
    [NonSerialized]
    private Dictionary<string, object> _runtimeCache;  // ?? Explicitly not serialized
    
    // Or use [IgnoreDataMember] with DataContract
}
```

## Best Practices

### ? DO Use These Types

```csharp
// Primitives
int, long, double, decimal, bool, string, DateTime, TimeSpan, Guid

// Collections with specific types
List<string>, Dictionary<string, int>, int[], etc.

// Strongly-typed records/classes
public record Address { ... }
public class Person { ... }

// Nullable value types
int?, DateTime?, etc.
```

### ? DON'T Use These Types

```csharp
// Direct object type
object value;

// Collections with object element
Dictionary<string, object> tags;
List<object> items;
object[] array;

// IntPtr/UIntPtr
IntPtr ptr;
UIntPtr uptr;
```

## Testing Recommendations

### Test Case 1: Verify Partial Serialization

```csharp
[DataContract]
public class MixedTypeRecord
{
    [DataMember]
    public string Name { get; set; }  // Should serialize
    
    [DataMember]
    public Dictionary<string, object> Metadata { get; set; }  // Should be skipped
    
    [DataMember]
    public int Count { get; set; }  // Should serialize
}

// Test:
var record = new MixedTypeRecord 
{ 
    Name = "Test", 
    Metadata = new() { ["key"] = "value" },
    Count = 42 
};

// After checkpoint/restore:
// - Name = "Test" ?
// - Metadata = null ?? (not serialized)
// - Count = 42 ?
```

### Test Case 2: Verify Complete Rejection

```csharp
// This should fail ValidateTypeForSerializer()
public record AllObjectRecord
{
    public object Value1 { get; init; }
    public object Value2 { get; init; }
}
// ? Should throw: all properties unsupported
```

## Comparison with KQL Dynamic Type

### KQL `dynamic` Type
```csharp
// In KqlDynamicRecord:
public Dictionary<string, object> Fields { get; }  // Handled by KqlSurrogate

// Special handling:
public class KqlDynamicRecordSurrogate : ISurrogateProvider
{
    // Custom serialization logic for Dictionary<string, object>
    // Converts to JSON before serialization
}
```

### Standard C# Types
```csharp
// In TypedRecordTypes:
public record MetricEvent
{
    public Dictionary<string, string> Tags { get; init; }  // ? Works!
    // string values are serializable, so this works
}
```

## Migration Guide

If you have existing types with `Dictionary<string, object>`:

### Step 1: Identify Affected Types

```bash
# Search for problematic patterns
grep -r "Dictionary<string, object>" *.cs
grep -r "List<object>" *.cs
grep -r "object\[\]" *.cs
```

### Step 2: Choose Migration Strategy

**Option A: Accept Partial Serialization**
- Do nothing
- Field won't be checkpointed
- Runtime state only

**Option B: Convert to Strongly-Typed**
```csharp
// Before:
public Dictionary<string, object> Config { get; init; }

// After:
public AppConfig Config { get; init; }

public record AppConfig
{
    public string Host { get; init; }
    public int Port { get; init; }
    public bool EnableSSL { get; init; }
}
```

**Option C: Use JSON Serialization**
```csharp
// Before:
public Dictionary<string, object> Metadata { get; init; }

// After:
[IgnoreDataMember]
public Dictionary<string, object> Metadata { get; set; }

[DataMember]
private string MetadataJson
{
    get => JsonSerializer.Serialize(Metadata);
    set => Metadata = JsonSerializer.Deserialize<Dictionary<string, object>>(value);
}
```

## Summary

### What Changed
- ? Added `IsUnsupportedForSerialization()` helper
- ? Deep type checking for collections and arrays
- ? Graceful filtering of unsupported properties
- ? No breaking changes to existing code

### What's Now Possible
- ? Types with **some** unsupported properties can serialize (partial)
- ? No more `SerializationException` for `Dictionary<string, object>`
- ? Better error messages (property skipped vs. full failure)

### What's Still Restricted
- ? Direct `object` fields still not supported
- ? `Dictionary<string, object>` values won't be checkpointed
- ? Runtime type information not preserved

### Recommendation
For production code with checkpoint requirements:
1. **Use strongly-typed alternatives** when possible
2. **Use KQL dynamic records** if schema varies
3. **Use JSON serialization** for truly dynamic data
4. **Accept partial serialization** for auxiliary metadata

The fix ensures Trill samples continue to work while preventing serialization errors!
