# Main Menu - Complete Options Summary

## Updated Main Menu Structure

```
??????????????????????????????????????????????????????????????
?      Trill Event Receiver - Mode Selection                ?
??????????????????????????????????????????????????????????????

Choose receiver mode:

  1. Local On-Premises Mode (default)
     - No Azure dependencies
     - Local filesystem checkpointing
     - Simulated event processing

  2. Azure Event Hub Mode (original)
     - Requires Azure Event Hub connection
     - Uses Azure storage for checkpoints

  3. Dynamic Event Processing Mode
     - Uses dynamic objects (ExpandoObject)
     - Flexible, decoupled aggregation logic
     - Runtime-configurable aggregations

  4. KQL Schema-Based Processing Mode
     - Define schemas using KQL syntax
     - Type-safe dynamic event processing
     - Full checkpointing support

  5. KQL Tests & Validation
     - Run comprehensive test suite
     - Validate schema parsing and processing

  6. Strongly-Typed Records & Classes       ? NEW!
     - C# records with nested types
     - Inheritance hierarchies
     - Complex composition patterns

  7. Exit

Enter your choice (1-7) [default: 1]:
```

## Option Details

### ? Option 1: Local On-Premises Mode
**File:** `LocalReceiver.cs`
**Features:**
- No cloud dependencies
- Local filesystem for checkpoints
- Simulated event stream
- Three aggregation modes (Simple Stats, Custom Fields, Multi-Metric)

**Use Case:** Development, testing, demos without Azure

---

### ? Option 2: Azure Event Hub Mode
**File:** `AzureEventHubReceiver.cs`
**Features:**
- Connects to Azure Event Hub
- Azure Blob Storage for checkpoints
- Production-ready streaming
- Distributed processing

**Use Case:** Production deployments with Azure infrastructure

---

### ? Option 3: Dynamic Event Processing Mode
**File:** `DynamicReceiver.cs`
**Features:**
- `ExpandoObject` / `FlexiblePayload` based
- Runtime schema flexibility
- Configurable aggregation modes
- No compile-time schema required

**Use Case:** Exploratory analysis, changing schemas, ad-hoc queries

**Key Types:**
- `FlexiblePayload` - Wrapper around Dictionary
- `DynamicEventProcessor` - Generic processor for dynamic data
- `AggregationMode` - Enum (SimpleStats, CustomFields, MultiMetric)

---

### ? Option 4: KQL Schema-Based Processing Mode
**File:** `KqlProcessorSample.cs`
**Features:**
- Define schemas using KQL `.create table` syntax
- Type-safe access to dynamic data
- Support for all 10 standard KQL data types
- Full checkpoint/restore capability
- Interactive parameter configuration

**Use Case:** Azure Data Explorer migrations, Kusto-style queries, structured dynamic data

**Key Components:**
- `KqlSchemaParser` - Parses KQL table definitions
- `KqlDynamicRecord` - Strongly-typed dynamic record
- `KqlEventProcessor` - Processor with schema validation
- `KqlQueryConfig` - Query configuration (CountOnly, FieldAggregation, MultiMetric)

**Supported Types:**
```
bool, datetime, decimal, dynamic, guid, int, long, real, string, timespan
+ Aliases: boolean, date, uniqueid, uuid, double, time
```

**Examples:**
1. Simple event counting
2. Field aggregation (sum, avg)
3. Multi-metric analysis (count, sum, avg, min, max)
4. Complex schema with checkpoint demo

---

### ? Option 5: KQL Tests & Validation
**File:** `KqlTests.cs`
**Features:**
- Comprehensive test suite
- Schema parsing validation
- All data types tested
- Type alias verification
- Null handling tests
- Complex dynamic structures

**Tests:**
1. Schema parser (3 syntax styles)
2. All standard KQL data types
3. All type aliases
4. Dynamic record builder
5. Null value handling
6. Complex nested dynamic types

**Use Case:** Validation, regression testing, learning

---

### ? Option 6: Strongly-Typed Records & Classes **[NEW]**
**File:** `TypedRecordSample.cs`
**Features:**
- C# `record` types with `init` properties
- Complex nested structures
- Inheritance hierarchies (abstract base classes)
- Composition patterns
- Collections (`List<T>`, `Dictionary<K,V>`)
- Enums for type safety
- Generic processor for any payload type
- Full checkpoint/restore support

**Use Case:** Production applications, known schemas, maximum performance, type safety

**Type Categories Demonstrated:**

#### 1. Simple Records
```csharp
record SensorReading {
    SensorId: string,
    Timestamp: DateTime,
    Temperature: double,
    Humidity: double,
    Location: GeoLocation (struct)
}
```

#### 2. Nested Structures
```csharp
record OrderEvent {
    OrderId: Guid,
    Items: List<OrderItem>,
    ShippingAddress: Address,
    Payment: PaymentInfo
}
```

#### 3. Inheritance Hierarchy
```csharp
abstract class VehicleEvent { ... }
    ?? CarEvent
    ?? TruckEvent
    ?? MotorcycleEvent
```

#### 4. Complex Composition
```csharp
record Transaction {
    TransactionId: Guid,
    Amount: decimal,
    Merchant: MerchantInfo (nested),
    Type: TransactionType (enum)
}
```

#### 5. Dictionary Properties
```csharp
record MetricEvent {
    Tags: Dictionary<string, string>
}
```

**5 Complete Examples:**
1. Simple records (sensor readings)
2. Nested structures (e-commerce orders)
3. Inheritance (vehicle tracking)
4. Complex aggregation (financial transactions)
5. Checkpoint/restore (metrics with tags)

**Key Components:**
- `TypedEventProcessor<TPayload>` - Generic processor for any strongly-typed payload
- `TypedQueryObserver<TInputType>` - Observer with reflection-based output formatting
- Rich type definitions in `TypedRecordTypes.cs`
- Visual UML-like documentation in sample output

**Advantages:**
- ? **Best Performance** - No boxing/unboxing overhead
- ? **Full IntelliSense** - IDE autocomplete and navigation
- ? **Compile-time Safety** - Typos caught at compile time
- ? **Refactoring Support** - Rename properties automatically
- ? **Type Safety** - Enums, nullability, constraints
- ? **Best for Production** - Fixed schemas, large-scale systems

---

### ? Option 7: Exit
Self-explanatory!

---

## Comparison Matrix

| Feature | Option 1 (Local) | Option 3 (Dynamic) | Option 4 (KQL) | Option 6 (Typed) |
|---------|-----------------|-------------------|----------------|------------------|
| **Schema** | Fixed | Runtime | KQL Definition | C# Types |
| **Type Safety** | ? Strong | ?? Runtime | ? Validated | ? Compile-time |
| **Performance** | ? Fast | ?? Slower (boxing) | ? Fast | ? Fastest |
| **Flexibility** | ?? Limited | ? High | ? Medium | ?? Fixed |
| **IntelliSense** | ? Full | ? None | ? Schema-based | ? Full |
| **Refactoring** | ? Yes | ? No | ?? Manual | ? Automatic |
| **Learning Curve** | Easy | Easy | Medium | Easy-Medium |
| **Best For** | Demos | Exploration | Azure migrations | Production |
| **Checkpoints** | ? Yes | ? Yes | ? Yes | ? Yes |

## Quick Selection Guide

**Choose Option 1** if:
- You want to get started quickly
- No Azure access/account
- Learning Trill basics
- Demo/presentation mode

**Choose Option 3** if:
- Schema changes frequently
- Exploratory data analysis
- Don't know schema upfront
- Ad-hoc queries

**Choose Option 4** if:
- Migrating from Azure Data Explorer
- Already using KQL syntax
- Need schema validation
- Working with Kusto-style data

**Choose Option 6** if:
- Building production applications
- Schema is known and stable
- Need maximum performance
- Want full IDE support
- Large-scale systems

**Choose Option 2** only if:
- Deploying to Azure
- Have Event Hub infrastructure
- Production streaming workload

**Choose Option 5** for:
- Validating KQL implementation
- Learning KQL features
- Testing schema changes

## Implementation Files Summary

### Core Trill Files (No changes needed)
- ? `QueryContainer.cs` - Container for checkpointable queries
- ? `Process.cs` - Running query with checkpoint capability
- ? `Pipe.cs` - Base pipe class
- ? `Checkpointable.cs` - Checkpoint base class

### Sample Files

#### Local/Azure (Original)
- `LocalReceiver.cs` - Local on-premises mode
- `AzureEventHubReceiver.cs` - Azure Event Hub mode
- `LocalEventProcessor.cs` - Local processor implementation
- `EventProcessor.cs` - Base event processor

#### Dynamic Processing (Added)
- `DynamicReceiver.cs` - Dynamic event processing menu/runner
- `DynamicEventProcessor.cs` - Processor for FlexiblePayload
- `FlexiblePayload.cs` - Wrapper for dynamic data

#### KQL Processing (Added)
- `KqlProcessorSample.cs` - Main sample runner with 4 examples
- `Kql/KqlSchemaParser.cs` - Parse KQL table definitions
- `Kql/KqlEventProcessor.cs` - Schema-validated event processor
- `Kql/KqlDynamicRecord.cs` - Dynamic record with schema
- `Kql/KqlDynamicRecordBuilder.cs` - Fluent builder API
- `Kql/KqlDynamicRecordSurrogate.cs` - Checkpoint serialization
- `Kql/KqlInteractiveSample.cs` - Interactive demo scenarios
- `Kql/KqlTests.cs` - Comprehensive test suite

#### Strongly-Typed (Added - NEW!)
- `TypedRecordSample.cs` - Main sample with 5 examples
- `TypedRecordTypes.cs` - Type definitions (records, classes, enums)
- `TypedEventProcessor.cs` - Generic processor for any typed payload

#### Documentation
- `TYPED_RECORDS_GUIDE.md` - Complete guide with UML diagrams
- `KQL_TYPE_IMPLEMENTATION_COMPLETE.md` - KQL type coverage
- `KQL_SCHEMA_GUIDE.md` - KQL schema parsing guide
- Various implementation summaries

### Main Entry Point
- **`Program.cs`** - ? Updated with all 7 options

## Running the Samples

### Quick Start
```bash
# Build
dotnet build TrillSamples/src/EventHubReceiver/EventHubReceiver.csproj

# Run
dotnet run --project TrillSamples/src/EventHubReceiver/EventHubReceiver.csproj
```

### Interactive Menu
```
1. Press Enter for default (Local mode)
2. Or type option number (1-7)
3. Follow on-screen prompts
4. Each sample returns to menu when done
```

## Summary of Additions

### What Was Added in This Session

1. ? **Strongly-Typed Records Sample** (Option 6)
   - 5 complete examples
   - Generic processor
   - Visual UML documentation
   - Best practices guide

2. ? **Menu Integration**
   - Added Option 6 to main menu
   - Updated choice validation (1-7)
   - Added descriptive text

3. ? **Documentation**
   - `TYPED_RECORDS_GUIDE.md` - Comprehensive guide
   - This summary document

### Build Status
? **All code compiles successfully**
? **No errors or warnings**
? **StyleCop compliant**

## Benefits of Complete Sample Suite

### For Learning
- ? Simple ? Complex progression
- ? Multiple paradigms demonstrated
- ? Real-world patterns
- ? Visual documentation

### For Development
- ? Copy-paste ready code
- ? Best practices shown
- ? Error handling examples
- ? Checkpoint patterns

### For Production
- ? Performance comparisons
- ? Type safety options
- ? Scalability patterns
- ? Maintainability guidance

## Next Steps for Users

1. **Try Option 1** first (Local mode) - Get familiar with basics
2. **Explore Option 6** (Typed records) - See production patterns
3. **Experiment with Option 4** (KQL) - Learn schema flexibility
4. **Compare with Option 3** (Dynamic) - Understand tradeoffs
5. **Run Option 5** (Tests) - Validate understanding

## Conclusion

The EventHubReceiver sample now provides a **complete, production-ready suite** demonstrating:
- ? Multiple data modeling approaches
- ? Dynamic vs. strongly-typed tradeoffs
- ? Real-world streaming patterns
- ? Checkpoint/restore capabilities
- ? Performance optimization techniques
- ? Azure integration options

**All 7 menu options are functional and documented!** ??
