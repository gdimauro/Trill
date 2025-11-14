# Advanced Typed Records Guide - Complex Event Processing with Trill

## Overview

This guide demonstrates **advanced complex event processing (CEP)** patterns using strongly-typed C# records and classes with Trill. Building on the basic typed records concepts, this shows real-world patterns for multi-stream processing, correlation, and pattern detection.

## Key Concepts

### 1. Stream Joins

Join multiple typed streams to enrich events with reference data or correlate related events.

```csharp
// Define input types
public record UserAction
{
    public string UserId { get; init; }
    public string ActionType { get; init; }
    public DateTime Timestamp { get; init; }
}

public record UserProfile
{
    public string UserId { get; init; }
    public string Name { get; init; }
    public string MembershipTier { get; init; }
}

// Join streams
var enrichedActions = actionStream
    .Join(
        profileStream,
        action => action.UserId,      // Left key
        profile => profile.UserId,    // Right key
        (action, profile) => new UserActionEnriched
        {
            UserId = action.UserId,
            ActionType = action.ActionType,
            UserName = profile.Name,
            UserTier = profile.MembershipTier
        });
```

**Use Cases:**
- Enriching transaction events with customer profiles
- Correlating sensor readings with device metadata
- Matching orders with inventory data

### 2. Pattern Detection

Detect complex patterns and sequences in event streams using windowing and aggregation.

```csharp
public record FinancialTransaction
{
    public string AccountId { get; init; }
    public decimal Amount { get; init; }
    public string Country { get; init; }
}

// Detect fraud patterns
var fraudAlerts = transactionStream
    .TumblingWindowLifetime(TimeSpan.FromSeconds(5).Ticks)
    .Where(t => t.Amount > 1000m)
    .Aggregate(
        w => w.Count(),
        w => w.Sum(t => t.Amount),
        (count, totalAmount) => new FraudAlert
        {
            TransactionCount = count,
            TotalAmount = totalAmount,
            AlertLevel = count > 3 ? "HIGH" : "MEDIUM"
        });
```

**Patterns Detected:**
- **Velocity patterns**: High frequency of events in short time
- **Threshold violations**: Values exceeding limits
- **Anomaly detection**: Unusual combinations or sequences

### 3. Multi-Stream Correlation

Correlate multiple heterogeneous streams (different event types) based on time and key.

```csharp
// Three different sensor types
public record TemperatureReading
{
    public string DeviceId { get; init; }
    public double Value { get; init; }
    public DateTime Timestamp { get; init; }
}

public record PressureReading { /* ... */ }
public record VibrationReading { /* ... */ }

// Correlate all three
var healthSnapshot = temperatureStream
    .Join(pressureStream, t => t.DeviceId, p => p.DeviceId,
        (t, p) => new { Temp = t, Press = p })
    .Join(vibrationStream, tp => tp.Temp.DeviceId, v => v.DeviceId,
        (tp, v) => new DeviceHealthSnapshot
        {
            DeviceId = tp.Temp.DeviceId,
            Temperature = tp.Temp.Value,
            Pressure = tp.Press.Value,
            Vibration = v.Value,
            HealthScore = CalculateHealth(tp.Temp.Value, tp.Press.Value, v.Value)
        });
```

**Use Cases:**
- **IoT monitoring**: Correlate multiple sensor types
- **Trading systems**: Match trades, quotes, and market data
- **Security**: Correlate authentication, network, and application logs

### 4. Temporal Queries

Perform time-based analysis including session detection and duration calculations.

```csharp
public record ClickEvent
{
    public string UserId { get; init; }
    public string PageUrl { get; init; }
    public DateTime Timestamp { get; init; }
}

// Detect user sessions
var sessions = clickStream
    .TumblingWindowLifetime(TimeSpan.FromSeconds(5).Ticks)
    .Aggregate(
        w => w.Count(),
        w => w.Min(c => c.Timestamp.Ticks),
        w => w.Max(c => c.Timestamp.Ticks),
        (clickCount, sessionStart, sessionEnd) => new UserSession
        {
            ClickCount = clickCount,
            SessionStart = new DateTime(sessionStart),
            SessionEnd = new DateTime(sessionEnd),
            DurationSeconds = (sessionEnd - sessionStart) / TimeSpan.TicksPerSecond
        });
```

**Temporal Operations:**
- **Session windows**: Group events with inactivity gaps
- **Duration tracking**: Calculate time between events
- **Time-based filtering**: Events within specific time ranges

### 5. Complex Event Processing Pipelines

Chain multiple operators to create sophisticated processing pipelines.

```csharp
private static IStreamable<Empty, object> CreateComplexCEPQuery(
    IStreamable<Empty, SensorDataPoint> input)
{
    // Step 1: Filter
    var filtered = input.Where(s => s.Value >= 0 && s.Value <= 100);

    // Step 2: Transform/Enrich
    var enriched = filtered.Select(s => new
    {
        s.SensorId,
        s.Value,
        Status = s.Value > 80 ? "Critical" : "Normal",
        Normalized = s.Value / 100.0
    });

    // Step 3: Window and Aggregate
    var aggregated = enriched
        .TumblingWindowLifetime(TimeSpan.FromSeconds(5).Ticks)
        .Aggregate(
            w => w.Count(),
            w => w.Average(s => s.Value),
            w => w.Max(s => s.Value),
            w => w.Where(s => s.Status == "Critical").Count(),
            (count, avg, max, criticalCount) => (object)new SensorAnalytics
            {
                ReadingCount = count,
                AverageValue = avg,
                MaxValue = max,
                CriticalCount = criticalCount
            });

    return aggregated;
}
```

**Pipeline Stages:**
1. **Filter**: Remove invalid/irrelevant events
2. **Transform**: Enrich with computed fields
3. **Aggregate**: Summarize over windows
4. **Pattern Match**: Detect complex sequences

## Advanced Type Patterns

### Joining with Reference Data

```csharp
// Static reference data
var profiles = new[]
{
    new UserProfile { UserId = "U1", Name = "Alice", Tier = "Gold" },
    new UserProfile { UserId = "U2", Name = "Bob", Tier = "Silver" }
};

// Convert to streamable (point events)
var profileStream = profiles
    .Select(p => StreamEvent.CreatePoint(DateTime.UtcNow.Ticks, p))
    .ToObservable()
    .ToStreamable();

// Join with streaming events
var enriched = actionStream.Join(profileStream, ...);
```

### Multi-Level Aggregation

```csharp
// First level: Per-device aggregation
var deviceStats = sensorStream
    .GroupBy(s => s.DeviceId)
    .TumblingWindowLifetime(TimeSpan.FromSeconds(10).Ticks)
    .Aggregate(w => w.Count(), w => w.Average(s => s.Value),
        (count, avg) => new { DeviceId = /* ... */, Count = count, Avg = avg });

// Second level: Cross-device aggregation
var fleetStats = deviceStats
    .TumblingWindowLifetime(TimeSpan.FromSeconds(30).Ticks)
    .Aggregate(
        w => w.Count(),        // Number of devices
        w => w.Average(d => d.Avg),  // Overall average
        (deviceCount, overallAvg) => new FleetStats { ... });
```

### Correlation with Time Tolerance

```csharp
// Allow events to be within ±1 second for correlation
var correlatedStream = stream1
    .AlterEventLifetime(TimeSpan.FromSeconds(1).Ticks) // Expand event duration
    .Join(stream2, ...);
```

## Best Practices

### 1. Type Safety

? **DO**: Use strongly-typed records
```csharp
public record TemperatureReading
{
    public string SensorId { get; init; }
    public double Celsius { get; init; }
}
```

? **DON'T**: Use dynamic types unless necessary
```csharp
dynamic reading = new ExpandoObject(); // Loses type safety
```

### 2. Immutability

? **DO**: Use `init` for record properties
```csharp
public record SensorData
{
    public string Id { get; init; }     // Immutable
    public double Value { get; init; }
}
```

? **DON'T**: Use mutable properties in stream processing
```csharp
public class SensorData
{
    public string Id { get; set; }  // Mutable - avoid
}
```

### 3. Join Performance

? **DO**: Join on indexed/partitioned keys
```csharp
var result = stream1.Join(stream2,
    s1 => s1.DeviceId,    // Use partition keys
    s2 => s2.DeviceId,
    (s1, s2) => ...);
```

? **DON'T**: Join without appropriate keys
```csharp
var result = stream1.Join(stream2,
    s1 => true,  // Cartesian product - very slow!
    s2 => true,
    (s1, s2) => ...);
```

### 4. Window Sizing

? **DO**: Choose appropriate window sizes
```csharp
// For real-time: small windows
.TumblingWindowLifetime(TimeSpan.FromSeconds(5).Ticks)

// For batch: larger windows
.TumblingWindowLifetime(TimeSpan.FromMinutes(15).Ticks)
```

### 5. Row-Based Execution

When using complex types, enable row-based execution:

```csharp
// At application start
Config.ForceRowBasedExecution = true;
```

**When to use:**
- Abstract base classes with inheritance
- Complex nested structures
- Large collection properties

## Common Patterns

### 1. Event Enrichment

```csharp
var enriched = events
    .Join(referenceData,
        e => e.CustomerId,
        r => r.CustomerId,
        (e, r) => new EnrichedEvent
        {
            // Event fields
            TransactionId = e.TransactionId,
            Amount = e.Amount,
            // Reference data fields
            CustomerName = r.Name,
            CustomerTier = r.Tier
        });
```

### 2. Anomaly Detection

```csharp
var anomalies = sensorStream
    .TumblingWindowLifetime(TimeSpan.FromMinutes(1).Ticks)
    .Aggregate(
        w => w.Average(s => s.Value),
        w => w.StdDev(s => s.Value),  // If available
        (avg, stddev) => new { Avg = avg, StdDev = stddev })
    .Join(sensorStream,
        stats => true,
        reading => true,
        (stats, reading) => new
        {
            reading.SensorId,
            reading.Value,
            IsAnomaly = Math.Abs(reading.Value - stats.Avg) > 3 * stats.StdDev
        })
    .Where(r => r.IsAnomaly);
```

### 3. Session Detection

```csharp
// Detect sessions with 30-second timeout
var sessions = clickStream
    .GroupBy(c => c.UserId)
    .TumblingWindowLifetime(TimeSpan.FromSeconds(30).Ticks)
    .Aggregate(
        w => w.Count(),
        w => w.Min(c => c.Timestamp.Ticks),
        w => w.Max(c => c.Timestamp.Ticks),
        (count, start, end) => new Session
        {
            ClickCount = count,
            StartTime = new DateTime(start),
            EndTime = new DateTime(end)
        });
```

### 4. Top-N Query

```csharp
// Find top 5 devices by error count per minute
var topDevices = errorStream
    .TumblingWindowLifetime(TimeSpan.FromMinutes(1).Ticks)
    .GroupBy(e => e.DeviceId)
    .Aggregate(w => w.Count(), (deviceId, count) => new { DeviceId = deviceId, Count = count })
    .Select(window => window
        .OrderByDescending(d => d.Count)
        .Take(5));
```

## Performance Considerations

### 1. Memory Management

- **Window size**: Larger windows = more memory
- **State retention**: Join state grows with window duration
- **Collection properties**: Avoid large lists in events

### 2. Throughput Optimization

- **Parallel processing**: Use `.SetProperty().IsColumnar = true` when possible
- **Batching**: Process events in batches for better throughput
- **Checkpointing**: Regular checkpoints prevent memory buildup

### 3. Latency Optimization

- **Small windows**: Reduce latency with smaller tumbling windows
- **Punctuations**: Use punctuations to trigger output immediately
- **Streaming joins**: Prefer streaming joins over batch joins

## Testing Strategies

### Unit Testing Join Logic

```csharp
[Fact]
public void TestUserActionEnrichment()
{
    var actions = new[] {
        new UserAction { UserId = "U1", ActionType = "Login" }
    }.ToObservableStreamable();

    var profiles = new[] {
        new UserProfile { UserId = "U1", Name = "Alice" }
    }.ToObservableStreamable();

    var enriched = actions.Join(profiles, ...);
    
    var results = enriched.ToStreamEventObservable().ToEnumerable().ToList();
    
    Assert.Single(results);
    Assert.Equal("Alice", results[0].Payload.UserName);
}
```

### Integration Testing

```csharp
[Fact]
public async Task TestComplexCEPPipeline()
{
    var processor = new TypedEventProcessor<SensorDataPoint>(
        "test-partition",
        testCheckpointDir,
        CreateComplexCEPQuery);

    processor.Initialize();

    // Send test events
    var testEvents = GenerateTestData(100);
    foreach (var evt in testEvents)
    {
        processor.ProcessEvent(evt);
    }

    processor.Flush();
    await Task.Delay(1000);

    // Verify results
    var results = GetProcessedResults();
    Assert.NotEmpty(results);
}
```

## Advanced Examples Summary

The `TypedRecordAdvancedSample.cs` demonstrates:

1. **Stream Joins** (`RunStreamJoinExample`)
   - Inner join of user actions and profiles
   - Event enrichment with reference data
   - **Important**: Subscribe first, then send events

2. **Pattern Detection** (`RunPatternDetectionExample`)
   - Fraud detection with velocity checks
   - Windowed aggregation with filtering
   - Non-blocking subscription pattern

3. **Multi-Stream Correlation** (`RunMultiStreamCorrelationExample`)
   - Three-way join of sensor streams
   - Health score calculation from multiple inputs
   - Async event handling

4. **Temporal Queries** (`RunTemporalQueryExample`)
   - Session detection with windowing
   - Duration calculation between events
   - Time-based aggregation

5. **Complex CEP** (`RunComplexEventProcessingExample`)
   - Multi-stage pipeline: Filter ? Transform ? Aggregate
   - Pattern matching with checkpointing

6. **Checkpointing & State Recovery** (`RunCheckpointingExample`) **NEW**
   - **Phase 1**: Process initial batch and create checkpoint
   - **Phase 2**: Simulate crash and recovery from checkpoint
   - **Phase 3**: Final verification of state continuity
   - Demonstrates full checkpoint/restore lifecycle

### Critical Pattern: Non-Blocking Subscriptions

?? **Important**: When working with observables and subjects, always follow this pattern:

```csharp
// 1. Create subjects and streamables
var subject = new Subject<StreamEvent<MyEvent>>();
var stream = subject.ToStreamable();

// 2. Build query
var query = stream.Where(...).Select(...);

// 3. Subscribe FIRST (non-blocking)
var subscription = query.ToStreamEventObservable().Subscribe(e => 
{
    if (e.IsData)
    {
        Console.WriteLine($"Result: {e.Payload}");
    }
});

// 4. THEN send events
foreach (var evt in events)
{
    subject.OnNext(evt);
}

// 5. Complete and cleanup
subject.OnCompleted();
Thread.Sleep(1000); // Allow processing
subscription.Dispose();
```

? **Don't do this** (will hang):
```csharp
// This blocks waiting for completion before events are sent!
query.ToStreamEventObservable().ForEachAsync(e => { ... }).Wait();

// Then send events - BUT IT'S TOO LATE, already blocking above
subject.OnNext(event);
