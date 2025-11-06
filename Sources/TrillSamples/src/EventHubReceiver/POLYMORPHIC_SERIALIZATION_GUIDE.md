# Polymorphic Serialization: KnownType Attributes Required

## Error Encountered

```
System.Runtime.Serialization.SerializationException:
Could not find any matching known type for 'EventHubReceiver.VehicleEvent'.
```

## Root Cause

When serializing **abstract base classes** or **interfaces**, Trill's serializer needs to know about all **concrete derived types** at compile time for polymorphic serialization.

### Why This Is Required

```csharp
// Serializer sees this in memory:
StreamEvent<VehicleEvent>[] batch;

// But actual runtime instances are:
new CarEvent()       // Derived type 1
new TruckEvent()     // Derived type 2
new MotorcycleEvent() // Derived type 3

// Serializer needs to know:
// - What are ALL possible concrete types?
// - How to identify the actual type during deserialization?
// - How to reconstruct the correct derived instance?
```

Without `[KnownType]` attributes, the serializer:
1. ? Cannot discover derived types automatically
2. ? Cannot save type discriminator information
3. ? Cannot deserialize to correct concrete type
4. ? Throws `SerializationException` on first encounter

## Solution: Add [KnownType] Attributes

### Before (Error):

```csharp
// ? Missing KnownType attributes
public abstract class VehicleEvent
{
    public string VehicleId { get; set; }
    public DateTime Timestamp { get; set; }
    public double Speed { get; set; }
}

public class CarEvent : VehicleEvent { ... }
public class TruckEvent : VehicleEvent { ... }
public class MotorcycleEvent : VehicleEvent { ... }
```

**Result:** `SerializationException: Could not find any matching known type`

### After (Fixed):

```csharp
using System.Runtime.Serialization;

// ? Declare all derived types
[KnownType(typeof(CarEvent))]
[KnownType(typeof(TruckEvent))]
[KnownType(typeof(MotorcycleEvent))]
public abstract class VehicleEvent
{
    public string VehicleId { get; set; }
    public DateTime Timestamp { get; set; }
    public double Speed { get; set; }
}

public class CarEvent : VehicleEvent 
{
    public int NumberOfDoors { get; set; }
    public bool HasSunroof { get; set; }
}

public class TruckEvent : VehicleEvent 
{
    public int CargoWeight { get; set; }
    public int NumberOfAxles { get; set; }
}

public class MotorcycleEvent : VehicleEvent 
{
    public int EngineCC { get; set; }
    public bool HasSidecar { get; set; }
}
```

**Result:** ? Serialization succeeds, polymorphism preserved

## How It Works

### Serialization Process

```csharp
// 1. Encounter VehicleEvent reference
VehicleEvent vehicle = new CarEvent();

// 2. Check runtime type
Type actualType = vehicle.GetType(); // CarEvent

// 3. Look up known types from [KnownType] attributes
var knownTypes = typeof(VehicleEvent).GetCustomAttributes<KnownTypeAttribute>();
// Found: CarEvent, TruckEvent, MotorcycleEvent

// 4. Find matching known type
if (knownTypes.Contains(actualType))
{
    // 5. Write type discriminator
    writer.WriteTypeId(actualType);
    
    // 6. Serialize as CarEvent
    SerializeCarEvent(vehicle as CarEvent);
}
```

### Deserialization Process

```csharp
// 1. Read type discriminator
Type actualType = reader.ReadTypeId(); // CarEvent

// 2. Verify against known types
if (!knownTypes.Contains(actualType))
    throw new SerializationException();

// 3. Deserialize as correct type
var vehicle = DeserializeCarEvent();

// 4. Return as base type
return (VehicleEvent)vehicle;
```

## Patterns and Best Practices

### Pattern 1: Simple Hierarchy

```csharp
[KnownType(typeof(Dog))]
[KnownType(typeof(Cat))]
public abstract class Animal
{
    public string Name { get; set; }
}

public class Dog : Animal 
{
    public string Breed { get; set; }
}

public class Cat : Animal 
{
    public int LivesRemaining { get; set; }
}
```

### Pattern 2: Deep Hierarchy

```csharp
// Level 1: Base
[KnownType(typeof(MotorVehicle))]
[KnownType(typeof(Bicycle))]
public abstract class Vehicle { }

// Level 2: Intermediate (also abstract)
[KnownType(typeof(Car))]
[KnownType(typeof(Truck))]
public abstract class MotorVehicle : Vehicle { }

// Level 3: Concrete
public class Car : MotorVehicle { }
public class Truck : MotorVehicle { }
public class Bicycle : Vehicle { }
```

**Important:** Each abstract level needs its own `[KnownType]` attributes!

### Pattern 3: Interface-Based

```csharp
[KnownType(typeof(JsonPayload))]
[KnownType(typeof(XmlPayload))]
[KnownType(typeof(BinaryPayload))]
public interface IPayload
{
    byte[] GetBytes();
}

public class JsonPayload : IPayload { ... }
public class XmlPayload : IPayload { ... }
public class BinaryPayload : IPayload { ... }
```

### Pattern 4: Dynamic Known Types (Advanced)

```csharp
// Use method to compute known types at runtime
[KnownType("GetKnownTypes")]
public abstract class PluginBase
{
    private static IEnumerable<Type> GetKnownTypes()
    {
        // Discover all plugin types dynamically
        return Assembly.GetExecutingAssembly()
            .GetTypes()
            .Where(t => t.IsSubclassOf(typeof(PluginBase)) && !t.IsAbstract);
    }
}
```

## Common Mistakes

### ? Mistake 1: Forgetting New Derived Type

```csharp
[KnownType(typeof(CarEvent))]
[KnownType(typeof(TruckEvent))]
// ? Forgot to add BoatEvent!
public abstract class VehicleEvent { }

public class BoatEvent : VehicleEvent { } // Added later

// Result: SerializationException when BoatEvent is serialized
```

**Fix:** Add `[KnownType(typeof(BoatEvent))]`

### ? Mistake 2: Missing Intermediate Types

```csharp
[KnownType(typeof(SportsCar))] // ? Missing Car!
public abstract class Vehicle { }

public abstract class Car : Vehicle { }
public class SportsCar : Car { }

// Result: Error - Car is not in known types
```

**Fix:**
```csharp
[KnownType(typeof(Car))]
[KnownType(typeof(SportsCar))]
public abstract class Vehicle { }
```

### ? Mistake 3: Wrong Namespace

```csharp
namespace VehicleTypes
{
    [KnownType(typeof(CarEvent))] // ? Which CarEvent?
    public abstract class VehicleEvent { }
}

namespace Models
{
    public class CarEvent : VehicleTypes.VehicleEvent { }
}

namespace Legacy
{
    public class CarEvent : VehicleTypes.VehicleEvent { }
}

// Result: Ambiguous type reference!
```

**Fix:** Use fully qualified names:
```csharp
[KnownType(typeof(Models.CarEvent))]
[KnownType(typeof(Legacy.CarEvent))]
```

## Debugging Tips

### Tip 1: List All Known Types

```csharp
public static void DebugKnownTypes(Type baseType)
{
    var knownTypes = baseType.GetCustomAttributes<KnownTypeAttribute>()
        .SelectMany(a => 
            a.Type != null 
                ? new[] { a.Type }
                : (IEnumerable<Type>)baseType.GetMethod(a.MethodName, 
                    BindingFlags.Static | BindingFlags.NonPublic)
                    .Invoke(null, null));
    
    Console.WriteLine($"Known types for {baseType.Name}:");
    foreach (var type in knownTypes)
    {
        Console.WriteLine($"  - {type.FullName}");
    }
}

// Usage:
DebugKnownTypes(typeof(VehicleEvent));
```

### Tip 2: Verify Type Compatibility

```csharp
public static bool IsKnownTypeValid(Type baseType, Type derivedType)
{
    if (!derivedType.IsSubclassOf(baseType) && 
        !baseType.IsAssignableFrom(derivedType))
    {
        Console.WriteLine($"? {derivedType.Name} is not derived from {baseType.Name}");
        return false;
    }
    
    if (derivedType.IsAbstract)
    {
        Console.WriteLine($"?? {derivedType.Name} is abstract - might need its own [KnownType]s");
    }
    
    return true;
}
```

### Tip 3: Unit Test for Missing Known Types

```csharp
[Test]
public void AllDerivedTypes_Should_BeKnownTypes()
{
    var baseType = typeof(VehicleEvent);
    var knownTypes = baseType.GetCustomAttributes<KnownTypeAttribute>()
        .Select(a => a.Type)
        .ToHashSet();
    
    var derivedTypes = Assembly.GetExecutingAssembly()
        .GetTypes()
        .Where(t => t.IsSubclassOf(baseType) && !t.IsAbstract);
    
    var missingTypes = derivedTypes.Where(t => !knownTypes.Contains(t)).ToList();
    
    Assert.IsEmpty(missingTypes, 
        $"Missing [KnownType] attributes: {string.Join(", ", missingTypes.Select(t => t.Name))}");
}
```

## Performance Considerations

### Serialization Overhead

```csharp
// Without polymorphism (fastest):
public class CarEvent { } // ~10 ns per object

// With [KnownType] (slight overhead):
[KnownType(typeof(CarEvent))]
public abstract class VehicleEvent { }
// ~15 ns per object (type discriminator written)

// With many known types:
[KnownType(typeof(Type1))]
[KnownType(typeof(Type2))]
// ... 100 known types
[KnownType(typeof(Type100))]
// ~20 ns per object (larger type lookup table)
```

### Memory Impact

```
Type Discriminator Storage:
- 1 byte: Up to 255 known types (typical)
- 2 bytes: 256-65535 known types
- 4 bytes: > 65535 known types (rare)

Example with 1000 events:
- Without polymorphism: 0 bytes overhead
- With polymorphism: 1000-4000 bytes overhead
```

## Alternative Approaches

### Option 1: Avoid Inheritance (Composition)

```csharp
// Instead of inheritance:
public abstract class VehicleEvent { }
public class CarEvent : VehicleEvent { }

// Use composition:
public record VehicleEvent
{
    public VehicleType Type { get; init; }
    public CarDetails CarDetails { get; init; }
    public TruckDetails TruckDetails { get; init; }
}

public enum VehicleType { Car, Truck, Motorcycle }
```

**Pros:**
- ? No [KnownType] required
- ? Simpler serialization

**Cons:**
- ? Less type-safe
- ? More memory (all fields always present)

### Option 2: Use Type Discriminator Field

```csharp
[DataContract]
public class VehicleEvent
{
    [DataMember] public VehicleKind Kind { get; set; }
    [DataMember] public Dictionary<string, object> Properties { get; set; }
}

public enum VehicleKind { Car, Truck, Motorcycle }
```

**Pros:**
- ? No [KnownType] required
- ? Flexible schema

**Cons:**
- ? Lost compile-time type safety
- ? Manual property extraction

### Option 3: Separate Streams per Type

```csharp
// Instead of one stream with polymorphism:
IStreamable<Empty, VehicleEvent> mixedStream;

// Use multiple streams:
IStreamable<Empty, CarEvent> carStream;
IStreamable<Empty, TruckEvent> truckStream;
IStreamable<Empty, MotorcycleEvent> motorcycleStream;
```

**Pros:**
- ? No polymorphism needed
- ? Better performance

**Cons:**
- ? Multiple pipelines to manage
- ? Harder to query across types

## Summary

### ? When to Use [KnownType]

- Abstract base classes
- Interface-based polymorphism
- Plugin architectures
- Mixed-type event streams

### ? When to Avoid

- Simple single-type streams
- Performance-critical paths
- Very large type hierarchies (>100 types)
- Rapidly changing schemas

### Checklist for Polymorphic Types

```
? Add [KnownType] for each concrete derived type
? Include intermediate abstract types if multi-level
? Use fully qualified names if ambiguous
? Update attributes when adding new derived types
? Test serialization/deserialization of each type
? Verify checkpoint/restore with mixed types
? Document known type requirements
? Consider composition alternatives
```

## Complete Working Example

```csharp
using System;
using System.Runtime.Serialization;

namespace EventHubReceiver
{
    [KnownType(typeof(CarEvent))]
    [KnownType(typeof(TruckEvent))]
    [KnownType(typeof(MotorcycleEvent))]
    public abstract class VehicleEvent
    {
        public string VehicleId { get; set; }
        public DateTime Timestamp { get; set; }
        public double Speed { get; set; }
        public GeoLocation Position { get; set; }
    }

    public class CarEvent : VehicleEvent
    {
        public int NumberOfDoors { get; set; }
        public bool HasSunroof { get; set; }
    }

    public class TruckEvent : VehicleEvent
    {
        public int CargoWeight { get; set; }
        public int NumberOfAxles { get; set; }
    }

    public class MotorcycleEvent : VehicleEvent
    {
        public int EngineCC { get; set; }
        public bool HasSidecar { get; set; }
    }
}

// Usage:
var stream = observable.ToStreamable();
var query = stream
    .Where(v => v.Speed > 60)
    .Select(v => new
    {
        v.VehicleId,
        v.Speed,
        Type = v is CarEvent ? "Car" 
             : v is TruckEvent ? "Truck" 
             : "Motorcycle"
    });

// Checkpoint/Restore works! ?
```

This fix is essential for the Strongly-Typed Records sample to work correctly with inheritance hierarchies!
