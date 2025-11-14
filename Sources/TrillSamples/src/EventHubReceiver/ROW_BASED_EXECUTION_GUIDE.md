# Row-Based Execution for Abstract Types

## Problem Encountered

```
System.NullReferenceException: Object reference not set to an instance of an object.
  at Microsoft.StreamProcessing.SnapshotWindowHoppingPipeSimple`3.OnNext(StreamMessage`2 batch)
  Line 92: var colpayload = batch.payload.col;  // ? NullReferenceException
```

## Root Cause

When using **abstract base classes** or **complex nested types** with Trill:
- Trill attempts to generate **columnar batches** where the payload is decomposed into columns
- **Abstract classes cannot be columnarized** because:
  - Trill doesn't know which concrete type will be in the batch at compile time
  - Each derived type may have different fields
  - Cannot statically determine the column layout

Result: Trill falls back to **row-based execution**, but **some operators still try to access `.payload.col`** which is `null` in row mode.

## Solution: Force Row-Based Execution + Compatible Operators

### Step 1: Set Config Early

Set `Config.ForceRowBasedExecution = true` before creating streams with abstract types:

```csharp
public static void Run()
{
    // Force row-based execution for abstract types
    Config.ForceRowBasedExecution = true;
    
    // Now create and process streams with abstract types
    RunInheritanceExample();
}
```

### Step 2: Use Compatible Window Operators

**? AVOID:** `HoppingWindowLifetime` - Incompatible with row-based execution

```csharp
// ? This will throw NullReferenceException in row-based mode
return input
    .HoppingWindowLifetime(TimeSpan.FromSeconds(5).Ticks, TimeSpan.FromSeconds(1).Ticks)
    .Aggregate(...);
```

**? USE:** `TumblingWindowLifetime` - Compatible with row-based execution

```csharp
// ? This works in row-based mode
return input
    .TumblingWindowLifetime(TimeSpan.FromSeconds(5).Ticks)
    .Aggregate(...);
```

## Operator Compatibility Matrix

| Operator | Columnar Mode | Row-Based Mode |
|----------|---------------|----------------|
| `Where` | ? Works | ? Works |
| `Select` | ? Works | ? Works |
| `TumblingWindowLifetime` | ? Works | ? Works |
| `HoppingWindowLifetime` | ? Works | ? **Fails** (NullRef) |
| `SlidingWindowLifetime` | ? Works | ?? May fail |
| `SnapshotWindow` (Tumbling) | ? Works | ? Works |
| `SnapshotWindow` (Hopping) | ? Works | ? **Fails** (NullRef) |
| `Aggregate` (simple) | ? Works | ? Works |
| `Join` | ? Works | ? Works |
| `Union` | ? Works | ? Works |

## When to Use Row-Based Execution

### ? Required For:

1. **Abstract Base Classes**
```csharp
[KnownType(typeof(CarEvent))]
[KnownType(typeof(TruckEvent))]
public abstract class VehicleEvent  // ? Cannot columnarize
{
    public string VehicleId { get; set; }
    public double Speed { get; set; }
}
```

2. **Interface Types**
```csharp
[KnownType(typeof(JsonPayload))]
[KnownType(typeof(XmlPayload))]
public interface IPayload { }  // ? Cannot columnarize
```

3. **Complex Nested Structures** (sometimes)
```csharp
public record OrderEvent
{
    public List<OrderItem> Items { get; init; }  // ? May not columnarize well
    public Address ShippingAddress { get; init; }  // ? Deep nesting
    public PaymentInfo Payment { get; init; }
}
```

### ?? Optional For:

1. **Deep Inheritance Hierarchies**
2. **Types with Many Nested Objects**
3. **Collections of Complex Types**

### ? Not Needed For:

1. **Simple Records**
```csharp
public record SensorReading  // ? Can columnarize
{
    public string SensorId { get; init; }
    public double Temperature { get; init; }
}
```

2. **Sealed Classes**
```csharp
public sealed class CarEvent  // ? Can columnarize
{
    public string VehicleId { get; set; }
    public int NumberOfDoors { get; set; }
}
```

3. **Value Types / Structs**
```csharp
public struct GeoLocation  // ? Can columnarize
{
    public double Latitude { get; init; }
    public double Longitude { get; init; }
}
```

## Performance Implications

### Columnar Execution (Default)

**Advantages:**
- ? **Faster aggregations** - SIMD optimizations
- ? **Better compression** - Column-wise compression
- ? **Cache efficient** - Sequential memory access
- ? **Lower memory** - Better packing

**Performance:**
```
Simple aggregation on 1M events:
Columnar: ~100ms
Row-based: ~300ms
```

**Best for:**
- Large datasets
- Simple types
- Aggregation-heavy queries
- Analytic workloads

### Row-Based Execution (Forced)

**Advantages:**
- ? **Supports polymorphism** - Abstract classes, interfaces
- ? **Simpler code generation** - No column decomposition
- ? **Works with any type** - Including complex nested types

**Disadvantages:**
- ? **Slower aggregations** - No SIMD
- ? **More memory** - Object overhead
- ? **Cache misses** - Pointer chasing

**Performance:**
```
Same aggregation on 1M events:
Row-based: ~300ms (3x slower)
```

**Best for:**
- Polymorphic types
- Complex nested structures
- Smaller datasets
- Type flexibility > performance

## Code Examples

### Example 1: Simple Record (Columnar OK)

```csharp
// No need to force row-based
public record SensorReading
{
    public string SensorId { get; init; }
    public double Temperature { get; init; }
}

// Usage:
var query = input
    .HoppingWindowLifetime(5, 1)
    .Aggregate(w => w.Average(e => e.Temperature));

// Runs in columnar mode ?
// batch.Temperature.col[i] access works
```

### Example 2: Abstract Class (Row-Based Required)

```csharp
// Must force row-based
Config.ForceRowBasedExecution = true;

[KnownType(typeof(CarEvent))]
public abstract class VehicleEvent
{
    public double Speed { get; set; }
}

public class CarEvent : VehicleEvent
{
    public int Doors { get; set; }
}

// Usage:
IStreamable<Empty, VehicleEvent> input;  // Mixed Car/Truck/Motorcycle
var query = input
    .Where(v => v.Speed > 60)
    .Aggregate(w => w.Average(e => e.Speed));

// Runs in row-based mode ?
// batch.payload.col[i] access works (entire object)
```

### Example 3: Conditional Row-Based

```csharp
// Check at runtime
if (typeof(TPayload).IsAbstract || 
    typeof(TPayload).IsInterface ||
    !typeof(TPayload).CanRepresentAsColumnar())
{
    Config.ForceRowBasedExecution = true;
}

var query = CreateQuery<TPayload>();
```

## Detecting Row vs. Columnar Mode

### At Compile Time

```csharp
public static bool CanColumnarize<T>()
{
    var type = typeof(T);
    
    // Cannot columnarize if:
    if (type.IsAbstract) return false;
    if (type.IsInterface) return false;
    if (!type.CanRepresentAsColumnar()) return false;
    
    return true;
}
```

### At Runtime

```csharp
public void OnNext(StreamMessage<TKey, TPayload> batch)
{
    if (batch.payload.col == null)
    {
        // Row-based mode
        for (int i = 0; i < batch.Count; i++)
        {
            var payload = batch.payload.col[i];  // ? Will throw!
        }
    }
    else
    {
        // Columnar mode
        // Access individual fields
    }
}
```

## Best Practices

### ? DO:

1. **Set Config Early**
```csharp
// At the beginning of your program
Config.ForceRowBasedExecution = true;

// Then create all streams
var stream1 = input1.ToStreamable();
var stream2 = input2.ToStreamable();
```

2. **Document Why**
```csharp
// Force row-based execution because VehicleEvent is abstract
// and cannot be decomposed into columns
Config.ForceRowBasedExecution = true;
```

3. **Use for Entire Pipeline**
```csharp
// Once set, applies to all streams in the process
Config.ForceRowBasedExecution = true;

// All these use row-based
var q1 = stream1.Where(...);
var q2 = stream2.Aggregate(...);
var q3 = q1.Join(q2, ...);
```

### ? DON'T:

1. **Toggle Mid-Pipeline**
```csharp
// ? BAD: Inconsistent execution modes
Config.ForceRowBasedExecution = false;
var q1 = stream.Where(...);  // Columnar

Config.ForceRowBasedExecution = true;
var q2 = q1.Select(...);     // Row-based
// Conversion overhead!
```

2. **Use for Performance Testing**
```csharp
// ? BAD: Comparing apples to oranges
Config.ForceRowBasedExecution = true;
var time1 = Benchmark(query);

Config.ForceRowBasedExecution = false;
var time2 = Benchmark(query);  // Not valid comparison!
```

3. **Assume Default Behavior**
```csharp
// ? BAD: Relying on automatic fallback
// May work sometimes, crash other times
[KnownType(typeof(CarEvent))]
public abstract class VehicleEvent { }

// No Config.ForceRowBasedExecution = true
var query = input.Aggregate(...);  // May crash!
```

## Troubleshooting

### Symptom: NullReferenceException on `.col` Access

**Problem:**
```
NullReferenceException at batch.payload.col[i]
```

**Diagnosis:**
- Using abstract class or interface
- Trill fell back to row-based mode
- Operator still trying columnar access

**Solution:**
```csharp
Config.ForceRowBasedExecution = true;
```

### Symptom: "Cannot do columnar operation on polymorphic sets"

**Problem:**
```
StreamProcessingException: Cannot do columnar operation on polymorphic sets.
The value's type X does not match the declared payload type.
```

**Diagnosis:**
- Actual runtime type differs from declared type
- Polymorphic data without row-based mode

**Solution:**
```csharp
// Add KnownType attributes
[KnownType(typeof(ConcreteType1))]
[KnownType(typeof(ConcreteType2))]
public abstract class BaseType { }

// AND force row-based
Config.ForceRowBasedExecution = true;
```

### Symptom: Slower Than Expected Performance

**Problem:**
Queries on simple types running slower than benchmarks

**Diagnosis:**
Row-based mode enabled when not needed

**Solution:**
```csharp
// Remove unnecessary forcing
// Config.ForceRowBasedExecution = true;  // ? Remove if not needed

// Or be selective
if (PayloadIsAbstract)
    Config.ForceRowBasedExecution = true;
```

## Comparison Table

| Feature | Columnar | Row-Based |
|---------|----------|-----------|
| **Abstract Classes** | ? Not supported | ? Supported |
| **Interfaces** | ? Not supported | ? Supported |
| **Polymorphism** | ? Crashes | ? Works |
| **Simple Types** | ? Fast | ?? Slower |
| **Aggregations** | ? SIMD | ? Scalar |
| **Memory Usage** | ? Lower | ? Higher |
| **Code Gen** | Complex | Simple |
| **Flexibility** | ?? Limited | ? High |

## Summary

### When Abstract Types Are Used:

```csharp
// Always do this:
Config.ForceRowBasedExecution = true;

// Before creating streams:
var stream = observable.ToStreamable();
var query = stream.Where(...).Aggregate(...);
```

### Why It's Needed:

- ? Abstract classes cannot be columnarized
- ? Prevents NullReferenceException on `.col` access
- ? Enables polymorphic data processing
- ?? Trades performance for type flexibility

### Alternative Approaches:

If performance is critical and you have abstract types:

1. **Use separate streams per concrete type:**
```csharp
IStreamable<Empty, CarEvent> carStream;
IStreamable<Empty, TruckEvent> truckStream;
// Process separately, union if needed
```

2. **Use composition instead of inheritance:**
```csharp
public record Vehicle
{
    public VehicleType Type { get; init; }
    public CarData CarData { get; init; }    // Nullable
    public TruckData TruckData { get; init; } // Nullable
}
```

3. **Accept the performance tradeoff:**
```csharp
Config.ForceRowBasedExecution = true;
// Simplest solution, works for most use cases
