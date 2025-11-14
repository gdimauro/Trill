# TypedEventProcessor Advanced Enhancements Summary

This document summarizes the advanced concepts that have been added to the TypedEventProcessor sample.

## Files Added

### 1. TypedEventProcessorAdvancedSamples.cs
**Purpose:** Library of reusable query patterns demonstrating advanced Trill concepts

**Contains 12 Sample Queries:**

1. **GroupByAggregation** - Basic grouping and averaging
2. **TumblingWindowDebounce** - Non-overlapping 10-second windows with full statistics
3. **HoppingWindowAggregation** - Overlapping 30-second windows with 10-second hops
4. **SessionWindowDebounce** - Activity-based sessions with 5-second timeout
5. **AdvancedProjection** - Computed fields and enrichment
6. **MultiLevelGroupBy** - Hierarchical grouping (DeviceType ? DeviceId)
7. **FilterThenAggregate** - Pre-filtering before windowing
8. **SelectManyProjection** - Event expansion into multiple outputs
9. **TopKDevices** - Top-5 devices by average temperature
10. **DistinctDevices** - Unique device detection per window
11. **ComplexAggregation** - Multiple statistical metrics in one query
12. **ChainedTransformations** - Multi-step pipeline pattern

### 2. TYPED_PROCESSOR_ADVANCED_GUIDE.md
**Purpose:** Comprehensive documentation and learning guide

**Sections:**
- GroupBy and Aggregations (basic and multi-level)
- Window-based Debouncing (tumbling, hopping, session)
- Advanced Projections (computed fields, expansion, chaining)
- Complex Query Patterns (filters, top-k, distinct)
- Performance Considerations (window sizing, memory management)
- Usage Examples and Debugging Tips

### 3. TypedEventProcessorAdvancedDemo.cs
**Purpose:** Interactive runnable demonstration program

**Features:**
- Menu-driven interface
- 9 interactive demos
- Live data generation
- Visual output formatting
- Real-time execution with timing
- Educational console messages

## Concepts Demonstrated

### GroupBy Operations

**Basic GroupBy:**
```csharp
.GroupApply(
    e => e.DeviceId,
    deviceStream => deviceStream.Aggregate(w => w.Average(e => e.Temperature)),
    (groupKey, avgTemp) => new { DeviceId = groupKey.Key, AverageTemperature = avgTemp })
```

**Multi-Level GroupBy:**
```csharp
.GroupApply(
    e => e.DeviceType,
    typeStream => typeStream.GroupApply(
        e => e.DeviceId,
        deviceStream => deviceStream.Count(),
        (deviceKey, count) => new { ... }),
    (typeKey, deviceStats) => new { ... })
```

### Window Types (Debouncing Patterns)

#### Tumbling Windows
- **Characteristics:** Non-overlapping, fixed-size intervals
- **Use Case:** Periodic reports, batch processing
- **Code:** `.TumblingWindowLifetime(TimeSpan.FromSeconds(10).Ticks)`

#### Hopping Windows
- **Characteristics:** Overlapping windows with fixed hop size
- **Use Case:** Smooth trends, moving averages
- **Code:** `.HoppingWindowLifetime(windowSize: 30 ticks, hopSize: 10 ticks)`

#### Session Windows
- **Characteristics:** Dynamic, timeout-based sessions
- **Use Case:** User activity tracking, burst detection
- **Code:** `.SessionTimeoutWindow(timeoutTicks)`

### Advanced Projections

#### Computed Fields
```csharp
.Select(e => new
{
    e.Temperature,
    TemperatureFahrenheit = (e.Temperature * 9.0 / 5.0) + 32,
    IsHighTemp = e.Temperature > 30,
    ComfortIndex = ComputeComfortIndex(e.Temperature, e.Humidity)
})
```

#### Event Expansion (SelectMany)
```csharp
.SelectMany(e =>
{
    var alerts = new List<dynamic>();
    if (e.Temperature > 35)
        alerts.Add(new { AlertType = "HighTemperature", ... });
    if (e.Humidity > 80)
        alerts.Add(new { AlertType = "HighHumidity", ... });
    return alerts;
})
```

#### Chained Transformations
```csharp
inputStream
    .Where(e => e.Temperature >= 15 && e.Temperature <= 30)
    .Select(e => new { e.DeviceId, Category = GetCategory(e) })
    .TumblingWindowLifetime(windowSize)
    .GroupApply(...)
```

## Demo Usage

### Running the Interactive Demo

```bash
cd TrillSamples/src/EventHubReceiver
dotnet run --project EventHubReceiver.csproj
```

Choose from menu:
1. GroupBy Demo
2. Tumbling Window Demo
3. Hopping Window Demo
4. Session Window Demo
5. Advanced Projection Demo
6. Filter + Aggregate Demo
7. SelectMany Demo
8. Chained Transformations Demo
9. Complex Aggregation Demo

### Using in Your Own Code

```csharp
// Import the query factory
using EventHubReceiver;

// Create processor with desired query
var processor = new TypedEventProcessor<SensorReading>(
    partitionId: "my-partition",
    checkpointDirectory: "./checkpoints",
    queryFactory: TypedEventProcessorAdvancedSamples.TumblingWindowDebounce());

processor.Initialize();

// Process events
var evt = StreamEvent.CreateInterval(
    start: DateTime.UtcNow.Ticks,
    end: DateTime.UtcNow.AddSeconds(1).Ticks,
    payload: new SensorReading { DeviceId = "sensor-1", Temperature = 25.5, ... });

processor.ProcessEvent(evt);
processor.Flush();
processor.Dispose();
```

## Key Learning Points

### 1. GroupBy is Powerful
- Groups streams by key
- Applies aggregations per group
- Maintains separate state per group
- Scalable to high cardinality

### 2. Windows Control Time
- **Tumbling:** Batch processing, reports
- **Hopping:** Smooth analytics, trends
- **Session:** Activity tracking, bursts

### 3. Projections Transform Data
- **Select:** 1-to-1 transformation
- **SelectMany:** 1-to-many expansion
- **Chaining:** Multi-step pipelines

### 4. Aggregations Summarize
- Count, Sum, Average, Min, Max
- TopK for rankings
- Distinct for uniqueness
- Custom aggregates possible

## Performance Tips

### Window Sizing
- **Small windows (<10s):** Low latency, high overhead
- **Medium windows (10-60s):** Balanced
- **Large windows (>60s):** High throughput, higher latency

### Memory Management
- Row-based execution already enabled
- Checkpoint every 10 seconds (configurable)
- Monitor state size via `CurrentlyBufferedInputCount`

### Query Optimization
1. Filter early (before windows)
2. Use appropriate window type
3. Minimize state with incremental aggregates
4. Group on low-cardinality keys when possible

## Common Patterns

### Pattern 1: Real-Time Dashboard
```csharp
Hopping Window ? GroupBy Device ? Aggregate Stats ? Output
```

### Pattern 2: Alert Generation
```csharp
Filter Anomalies ? SelectMany ? Expand to Alerts ? Output
```

### Pattern 3: Periodic Report
```csharp
Tumbling Window ? GroupBy Category ? Aggregate ? Output
```

### Pattern 4: Session Analysis
```csharp
Session Window ? GroupBy User ? Aggregate Activity ? Output
```

## Troubleshooting

### No Output?
- ? Check punctuations are being sent
- ? Verify window alignment
- ? Ensure events are in order

### High Memory?
- ? Reduce window size
- ? Increase checkpoint frequency
- ? Check for unbounded groups

### Incorrect Results?
- ? Verify event timestamps
- ? Check window boundaries
- ? Review grouping keys

## Next Steps

1. **Explore** - Try each demo to understand behavior
2. **Experiment** - Modify window sizes and queries
3. **Build** - Create your own query patterns
4. **Optimize** - Monitor and tune performance
5. **Extend** - Combine patterns for complex scenarios

## Additional Resources

- **TypedEventProcessor.cs** - Core implementation
- **TYPED_RECORDS_GUIDE.md** - Type-safe processing basics
- **Trill GitHub** - https://github.com/microsoft/Trill
- **Trill Paper** - Academic foundation and algorithms

---

## Quick Reference Card

| Concept | Code Pattern | Use Case |
|---------|-------------|----------|
| **GroupBy** | `.GroupApply(keySelector, aggregator, resultSelector)` | Per-group statistics |
| **Tumbling** | `.TumblingWindowLifetime(duration)` | Periodic batches |
| **Hopping** | `.HoppingWindowLifetime(size, hop)` | Smooth trends |
| **Session** | `.SessionTimeoutWindow(timeout)` | Activity tracking |
| **Project** | `.Select(e => new { ... })` | Transform events |
| **Expand** | `.SelectMany(e => list)` | 1-to-many |
| **Filter** | `.Where(e => condition)` | Pre-filtering |
| **TopK** | `.TopK(selector, k)` | Rankings |
| **Distinct** | `.Distinct(selector)` | Uniqueness |
| **Aggregate** | `.Aggregate(w => w.Count())` | Summaries |

---

**Happy Streaming!** ??
