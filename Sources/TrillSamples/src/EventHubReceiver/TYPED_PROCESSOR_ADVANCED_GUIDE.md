# TypedEventProcessor Advanced Concepts Guide

This guide demonstrates advanced streaming query concepts with the `TypedEventProcessor`, including **GroupBy**, **Debounce patterns** (via windows), and **Advanced Projections**.

---

## Table of Contents

1. [Overview](#overview)
2. [GroupBy and Aggregations](#groupby-and-aggregations)
3. [Window-based Debouncing](#window-based-debouncing)
4. [Advanced Projections](#advanced-projections)
5. [Practical Examples](#practical-examples)
6. [Performance Tips](#performance-tips)

---

## Overview

The `TypedEventProcessor` supports sophisticated streaming patterns that allow you to:
- **Group events** by keys and compute aggregates per group
- **Window events** to create time-based batches (debouncing)
- **Transform events** with computed fields and expansions

### Basic Usage Pattern

```csharp
var processor = new TypedEventProcessor<TPayload>(
    partitionId: "my-partition",
    checkpointDirectory: "./checkpoints",
    queryFactory: MyAdvancedQuery);

processor.Initialize();

// Process events...
processor.ProcessEvent(evt);

processor.Flush();
processor.Dispose();
```

---

## GroupBy and Aggregations

### Concept 1: Basic GroupBy with Aggregation

Group events by a key field and compute aggregates within each group.

**Query Pattern:**
```csharp
public static Func<IStreamable<Empty, SensorReading>, IStreamable<Empty, SensorStats>> GroupByDeviceId()
{
    return inputStream => inputStream
        .GroupApply(
            e => e.SensorId,  // Group by sensor ID
            deviceStream => deviceStream.Aggregate(w => new SensorStats
            {
                ReadingCount = w.Count(),
                AverageTemperature = w.Average(e => e.Temperature),
                MaxTemperature = w.Max(e => e.Temperature),
                MinTemperature = w.Min(e => e.Temperature)
            }),
            (groupKey, stats) => stats);
}
```

**Use Cases:**
- Per-device statistics
- Per-customer metrics
- Per-region aggregation

### Concept 2: Multi-Level GroupBy

Create hierarchical groupings for nested analytics.

**Query Pattern:**
```csharp
public static Func<IStreamable<Empty, EnrichedSensorReading>, IStreamable<Empty, DeviceTypeStats>> MultiLevelGroupBy()
{
    return inputStream => inputStream
        // First level: Group by DeviceType
        .GroupApply(
            e => e.DeviceType,
            typeStream => typeStream
                // Second level: Group by SensorId within each DeviceType
                .GroupApply(
                    e => e.SensorId,
                    sensorStream => sensorStream.Count(),
                    (sensorKey, count) => new { SensorId = sensorKey.Key, Count = count }),
            (typeKey, sensorStats) => new DeviceTypeStats
            {
                DeviceType = typeKey.Key,
                SensorStats = sensorStats
            });
}
```

**Visual Representation:**
```
DeviceType
  ??? "Temperature"
  ?   ??? sensor-T1 ? 100 events
  ?   ??? sensor-T2 ? 150 events
  ??? "Humidity"
      ??? sensor-H1 ? 80 events
      ??? sensor-H2 ? 120 events
```

---

## Window-based Debouncing

Windows control **when** output is produced, implementing various debouncing strategies.

### Concept 3: Tumbling Windows (Non-Overlapping Batches)

Fixed-size windows that don't overlap - perfect for periodic reports.

**Query Pattern:**
```csharp
public static Func<IStreamable<Empty, SensorReading>, IStreamable<Empty, WindowStats>> TumblingWindowAggregation()
{
    return inputStream => inputStream
        .TumblingWindowLifetime(TimeSpan.FromSeconds(10).Ticks)
        .Aggregate(w => new WindowStats
        {
            WindowStart = DateTime.UtcNow,  // Computed at output time
            EventCount = w.Count(),
            AvgTemperature = w.Average(e => e.Temperature),
            MinTemperature = w.Min(e => e.Temperature),
            MaxTemperature = w.Max(e => e.Temperature)
        });
}
```

**Timeline Visualization:**
```
Time:     [0----10)[10---20)[20---30)[30---40)
Events:   xxx  xx  xx x xxx  x    xx  xxxx  x
Output:        ^        ^        ^        ^
```

**Key Characteristics:**
- ? No overlapping events
- ? Each event belongs to exactly one window
- ? Predictable output times
- ? Memory efficient

**When to Use:**
- Periodic reports (every N seconds/minutes)
- Batch processing
- Fixed-interval summaries

### Concept 4: Hopping Windows (Overlapping Batches)

Overlapping windows for smooth trends and moving averages.

**Query Pattern:**
```csharp
public static Func<IStreamable<Empty, SensorReading>, IStreamable<Empty, TrendStats>> HoppingWindowAggregation()
{
    return inputStream => inputStream
        .HoppingWindowLifetime(
            windowSize: TimeSpan.FromSeconds(30).Ticks,  // 30-second window
            hopSize: TimeSpan.FromSeconds(10).Ticks)      // 10-second hop
        .GroupApply(
            e => e.SensorId,
            deviceStream => deviceStream.Average(e => e.Temperature),
            (groupKey, avgTemp) => new TrendStats
            {
                SensorId = groupKey.Key,
                AverageTemperature = avgTemp,
                WindowSize = 30,
                HopSize = 10
            });
}
```

**Timeline Visualization:**
```
Time:    [0---------30)
              [10--------40)
                   [20--------50)
Events:  xxx xx xxx xx  xxx  xx x  x
Output:      ^      ^       ^
```

**Key Characteristics:**
- ? Smooth analytics
- ? Events appear in multiple windows
- ? Less sensitivity to window boundaries
- ?? Higher memory usage

**When to Use:**
- Moving averages
- Trend detection
- Smooth real-time dashboards

### Concept 5: Session Windows (Activity-based)

Dynamic windows that close after a period of inactivity.

**Query Pattern:**
```csharp
public static Func<IStreamable<Empty, UserEvent>, IStreamable<Empty, SessionInfo>> SessionWindowAnalysis()
{
    var timeoutTicks = TimeSpan.FromMinutes(5).Ticks;

    return inputStream => inputStream
        .GroupApply(
            e => e.UserId,
            userStream => userStream
                .SessionTimeoutWindow(timeoutTicks)
                .Aggregate(w => new SessionInfo
                {
                    EventCount = w.Count(),
                    AvgEventInterval = 0  // Would need custom calculation
                }),
            (groupKey, session) => new SessionInfo
            {
                UserId = groupKey.Key,
                EventsInSession = session.EventCount
            });
}
```

**Timeline Visualization:**
```
Events:  x  x x     (5min gap)     x x  x     (5min gap)     x
Session: [Session 1 ]              [Session 2]               [Session 3]
```

**Key Characteristics:**
- ? Dynamic window sizes
- ? Adapts to data patterns
- ? Natural activity boundaries
- ?? Unpredictable output times

**When to Use:**
- User session tracking
- Activity burst detection
- Idle timeout scenarios

---

## Advanced Projections

Transform and enrich events before processing.

### Concept 6: Computed Fields

Add derived fields to events.

**Query Pattern:**
```csharp
public static Func<IStreamable<Empty, SensorReading>, IStreamable<Empty, EnrichedReading>> ComputedFields()
{
    return inputStream => inputStream
        .Select(e => new EnrichedReading
        {
            // Original fields
            SensorId = e.SensorId,
            Temperature = e.Temperature,
            Humidity = e.Humidity,
            Timestamp = e.Timestamp,

            // Computed fields
            TemperatureFahrenheit = (e.Temperature * 9.0 / 5.0) + 32,
            IsHighTemp = e.Temperature > 30,
            IsHighHumidity = e.Humidity > 70,
            ComfortIndex = ComputeComfortIndex(e.Temperature, e.Humidity),
            AlertLevel = GetAlertLevel(e.Temperature, e.Humidity)
        });
}

private static double ComputeComfortIndex(double temp, double humidity)
{
    return temp + (humidity / 100.0) * 5;  // Simplified heat index
}

private static string GetAlertLevel(double temp, double humidity)
{
    if (temp > 35 || humidity > 80) return "CRITICAL";
    if (temp > 30 || humidity > 70) return "WARNING";
    return "NORMAL";
}
```

**Benefits:**
- Enrich data on the fly
- Categorize events
- Prepare for downstream processing

### Concept 7: Event Expansion (SelectMany)

One input event ? Multiple output events.

**Query Pattern:**
```csharp
public static Func<IStreamable<Empty, SensorReading>, IStreamable<Empty, Alert>> EventExpansion()
{
    return inputStream => inputStream
        .SelectMany(e =>
        {
            var alerts = new List<Alert>();

            // Generate alerts based on conditions
            if (e.Temperature > 35)
                alerts.Add(new Alert
                {
                    SensorId = e.SensorId,
                    AlertType = "HighTemperature",
                    Value = e.Temperature,
                    Severity = "Critical",
                    Timestamp = e.Timestamp
                });

            if (e.Humidity > 80)
                alerts.Add(new Alert
                {
                    SensorId = e.SensorId,
                    AlertType = "HighHumidity",
                    Value = e.Humidity,
                    Severity = "Warning",
                    Timestamp = e.Timestamp
                });

            if (e.Temperature < 10)
                alerts.Add(new Alert
                {
                    SensorId = e.SensorId,
                    AlertType = "LowTemperature",
                    Value = e.Temperature,
                    Severity = "Warning",
                    Timestamp = e.Timestamp
                });

            return alerts;
        });
}
```

**Use Cases:**
- Alert generation
- Event normalization
- Denormalization of nested data

### Concept 8: Chained Transformations

Build complex pipelines by chaining operations.

**Query Pattern:**
```csharp
public static Func<IStreamable<Empty, SensorReading>, IStreamable<Empty, ProcessedStats>> ChainedPipeline()
{
    return inputStream => inputStream
        // Step 1: Filter (remove outliers)
        .Where(e => e.Temperature >= 15 && e.Temperature <= 30)

        // Step 2: Enrich (add categories)
        .Select(e => new CategorizedReading
        {
            SensorId = e.SensorId,
            Temperature = e.Temperature,
            Humidity = e.Humidity,
            TempCategory = GetTemperatureCategory(e.Temperature),
            HumidityCategory = GetHumidityCategory(e.Humidity)
        })

        // Step 3: Window (10-second tumbling)
        .TumblingWindowLifetime(TimeSpan.FromSeconds(10).Ticks)

        // Step 4: Group by sensor
        .GroupApply(
            e => e.SensorId,
            sensorStream => sensorStream.Aggregate(w => new ProcessedStats
            {
                Count = w.Count(),
                AvgTemp = w.Average(e => e.Temperature)
            }),
            (groupKey, stats) => new ProcessedStats
            {
                SensorId = groupKey.Key,
                EventCount = stats.Count,
                AverageTemperature = stats.AvgTemp,
                ProcessedAt = DateTime.UtcNow
            });
}
```

**Pipeline Flow:**
```
Input Events
    ?
Filter (Where: 15-30°C)
    ?
Transform (Select: Add categories)
    ?
Window (10-second tumbling)
    ?
Group (By SensorId)
    ?
Aggregate (Count, Avg)
    ?
Output Stats
```

---

## Practical Examples

### Example 1: IoT Temperature Monitoring Dashboard

**Scenario:** Real-time temperature monitoring with alerts and trends.

```csharp
// Configure processor with multi-window analysis
var processor = new TypedEventProcessor<SensorReading>(
    partitionId: "temperature-monitor",
    checkpointDirectory: "./checkpoints/temp",
    queryFactory: input => input
        // Tumbling windows for reports (every 30 seconds)
        .TumblingWindowLifetime(TimeSpan.FromSeconds(30).Ticks)
        .GroupApply(
            e => e.SensorId,
            sensorStream => sensorStream.Aggregate(w => new
            {
                Count = w.Count(),
                AvgTemp = w.Average(e => e.Temperature),
                MaxTemp = w.Max(e => e.Temperature),
                MinTemp = w.Min(e => e.Temperature)
            }),
            (groupKey, agg) => new SensorStats
            {
                SensorId = groupKey.Key,
                ReadingCount = agg.Count,
                AverageTemperature = agg.AvgTemp,
                MaxTemperature = agg.MaxTemp,
                MinTemperature = agg.MinTemp
            }));

processor.Initialize();

// Process incoming sensor data...
```

### Example 2: User Session Analysis

**Scenario:** Track user sessions with 5-minute timeout.

```csharp
var processor = new TypedEventProcessor<UserActivity>(
    partitionId: "user-sessions",
    checkpointDirectory: "./checkpoints/sessions",
    queryFactory: input => input
        .GroupApply(
            e => e.UserId,
            userStream => userStream
                .SessionTimeoutWindow(TimeSpan.FromMinutes(5).Ticks)
                .Aggregate(w => new UserSession
                {
                    ActionCount = w.Count(),
                    FirstAction = w.Min(e => e.Timestamp),
                    LastAction = w.Max(e => e.Timestamp)
                }),
            (groupKey, session) => new UserSession
            {
                UserId = groupKey.Key,
                ActionCount = session.ActionCount,
                Duration = (session.LastAction - session.FirstAction).Ticks
            }));
```

### Example 3: Alert Generation System

**Scenario:** Generate alerts from sensor anomalies.

```csharp
var processor = new TypedEventProcessor<SensorReading>(
    partitionId: "alert-generator",
    checkpointDirectory: "./checkpoints/alerts",
    queryFactory: input => input
        .SelectMany(e =>
        {
            var alerts = new List<Alert>();

            if (e.Temperature > 35)
                alerts.Add(new Alert
                {
                    Type = "HighTemp",
                    Sensor = e.SensorId,
                    Value = e.Temperature,
                    Severity = "Critical"
                });

            if (e.Humidity > 80)
                alerts.Add(new Alert
                {
                    Type = "HighHumidity",
                    Sensor = e.SensorId,
                    Value = e.Humidity,
                    Severity = "Warning"
                });

            return alerts;
        }));
```

---

## Performance Tips

### Window Sizing Guidelines

| Window Size | Latency | Throughput | Memory | Use Case |
|-------------|---------|------------|--------|----------|
| < 10s       | Low     | Low        | Low    | Real-time alerts |
| 10-60s      | Medium  | Medium     | Medium | Dashboards |
| > 60s       | High    | High       | High   | Reports |

### Memory Management

**Best Practices:**
1. **Use appropriate window types:**
   - Tumbling ? Periodic reports
   - Hopping ? Smooth trends
   - Session ? Activity tracking

2. **Minimize state:**
   - Use incremental aggregates (Count, Sum, Avg)
   - Avoid storing full event collections
   - Consider approximate algorithms for large datasets

3. **Optimize GroupBy keys:**
   - Use primitive types when possible
   - Keep key objects small
   - Be aware of cardinality (number of unique groups)

4. **Chain operations efficiently:**
   - Filter early to reduce data volume
   - Group before expensive computations
   - Use Select for cheap transformations

### Checkpoint Strategy

The `TypedEventProcessor` checkpoints every 10 seconds by default:

```csharp
private static readonly TimeSpan CheckpointInterval = TimeSpan.FromSeconds(10);
```

**Tuning Considerations:**
- **Shorter intervals:** Faster recovery, higher overhead
- **Longer intervals:** Lower overhead, slower recovery
- Adjust based on throughput and recovery requirements

---

## Key Takeaways

1. **GroupBy** enables per-group analytics with minimal code
2. **Windows** control output timing and implement debouncing
3. **Projections** transform data on-the-fly
4. **Chaining** creates powerful multi-step pipelines
5. **TypedEventProcessor** handles checkpointing automatically

## Next Steps

1. Review [TypedEventProcessor.cs](TypedEventProcessor.cs) implementation
2. Explore [TypedRecordSample.cs](TypedRecordSample.cs) for basic examples
3. Experiment with different window sizes
4. Combine patterns for complex scenarios
5. Monitor performance and adjust accordingly

---

**Happy Streaming!** ??
