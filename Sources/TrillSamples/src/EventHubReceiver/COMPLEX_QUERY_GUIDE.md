# Complex Trill Query Examples

This document explains the complex query implementation in `LocalEventProcessor.cs` that demonstrates advanced Trill features including sliding windows, multiple aggregations, and real-time statistics.

## Query Overview

The query processes a stream of process working set measurements and computes five different statistics over sliding time windows:

1. **Count**: Number of events in the window
2. **Average**: Average working set size
3. **Sum**: Total working set across all events
4. **Minimum**: Smallest working set value
5. **Maximum**: Largest working set value

## Query Components

### 1. Sliding Window Configuration

```csharp
var windowSize = TimeSpan.FromSeconds(5).Ticks;
var slideSize = TimeSpan.FromSeconds(1).Ticks;

inputStream.HoppingWindowLifetime(windowSize, slideSize)
```

**What this means**:
- **Window Size**: 5 seconds - Each window looks at 5 seconds worth of data
- **Slide Size**: 1 second - New window starts every 1 second
- **Overlap**: Windows overlap by 4 seconds

**Example Timeline**:
```
Window 1: [00:00:00 - 00:00:05]
Window 2:     [00:00:01 - 00:00:06]  ? 4 seconds overlap
Window 3:         [00:00:02 - 00:00:07]
Window 4:             [00:00:03 - 00:00:08]
...
```

### 2. Multiple Aggregations

```csharp
.Aggregate(
    w => w.Count(),                      // Count events
    w => w.Average(e => (double)e),      // Average value
    w => w.Sum(e => e),                  // Sum of values
    w => w.Min(e => e),                  // Minimum value
    w => w.Max(e => e),                  // Maximum value
    (count, avg, sum, min, max) => new WindowStats
    {
        Count = count,
        Average = avg,
        Sum = sum,
        Min = min,
        Max = max
    })
```

**Benefits of Multiple Aggregations**:
- Computed in a single pass over the data
- Efficient memory usage
- All statistics updated atomically
- Checkpointable state

### 3. Custom Output Type

```csharp
private sealed class WindowStats
{
    public ulong Count { get; set; }     // Total events (unsigned)
    public double Average { get; set; }  // Mean value
    public long Sum { get; set; }        // Total sum
    public long Min { get; set; }        // Smallest value
    public long Max { get; set; }        // Largest value
}
```

**Why a custom class**?
- Type safety
- Serializable for checkpoints
- Clear property names
- Easy to extend with additional metrics

### 4. Formatted Output

```csharp
Console.WriteLine($"\n??????????????????????????????????????????????????????????????");
Console.WriteLine($"? [partition-0] Window #1");
Console.WriteLine($"? Time: 10:30:00 - 10:30:05");
Console.WriteLine($"??????????????????????????????????????????????????????????????");
Console.WriteLine($"? Count    :          5 events");
Console.WriteLine($"? Average  : 45,234,567.89 bytes");
Console.WriteLine($"? Sum      : 226,172,839 bytes");
Console.WriteLine($"? Minimum  : 43,123,456 bytes");
Console.WriteLine($"? Maximum  : 47,456,789 bytes");
Console.WriteLine($"??????????????????????????????????????????????????????????????");
```

## Sample Output

```
???????????????????????????????????????????????????????????
  Trill Local Event Receiver - On-Premises Mode
???????????????????????????????????????????????????????????

Checkpoint directory: C:\Users\...\AppData\Local\TrillCheckpoints

Clean start of query
LocalEventProcessor initialized. Partition: 'partition-0'
Local Event Receiver started
Processing events from simulated source...
Press Ctrl+C to stop.

Processed 10 events (WorkingSet: 45 MB)

??????????????????????????????????????????????????????????????
? [partition-0] Window #1
? Time: 15:23:10 - 15:23:15
??????????????????????????????????????????????????????????????
? Count    :          5 events
? Average  : 47,456,789.60 bytes
? Sum      : 237,283,948 bytes
? Minimum  : 45,123,456 bytes
? Maximum  : 49,234,567 bytes
??????????????????????????????????????????????????????????????

??????????????????????????????????????????????????????????????
? [partition-0] Window #2
? Time: 15:23:11 - 15:23:16
??????????????????????????????????????????????????????????????
? Count    :          5 events
? Average  : 47,856,234.40 bytes
? Sum      : 239,281,172 bytes
? Minimum  : 46,234,567 bytes
? Maximum  : 49,456,789 bytes
??????????????????????????????????????????????????????????????

Taking checkpoint at sequence 10
Checkpoint saved successfully
```

## Query Features Demonstrated

### 1. **Hopping Windows** (Sliding Windows)

Hopping windows allow overlapping time windows, perfect for:
- Smooth trend analysis
- Early anomaly detection
- Continuous monitoring
- Temporal pattern recognition

**Advantages over Tumbling Windows**:
- More frequent updates
- Better temporal resolution
- Smoother metrics transitions
- Reduced latency in trend detection

### 2. **Multiple Simultaneous Aggregations**

Computing multiple aggregations together is efficient because:
- Single pass through data
- Shared window state
- Atomic updates
- Consistent snapshots

**Typical Use Cases**:
- Dashboard metrics
- Real-time monitoring
- Alerting systems
- Performance analysis

### 3. **Type-Safe Results**

Using `WindowStats` class provides:
- IntelliSense support
- Compile-time checking
- Clear property names
- Easy serialization

### 4. **Checkpointable State**

The query state can be checkpointed:
- Window buffers
- Partial aggregations
- Event counts
- Min/max tracking

**Recovery Behavior**:
- Windows resume processing
- Aggregations continue accumulating
- No data loss
- Exactly-once semantics

## Customization Examples

### Example 1: Different Window Sizes

```csharp
// 10-second windows, sliding every 2 seconds
var windowSize = TimeSpan.FromSeconds(10).Ticks;
var slideSize = TimeSpan.FromSeconds(2).Ticks;
```

### Example 2: Additional Statistics

```csharp
.Aggregate(
    w => w.Count(),
    w => w.Average(e => (double)e),
    w => w.Sum(e => e),
    w => w.Min(e => e),
    w => w.Max(e => e),
    w => w.StandardDeviation(e => (double)e),  // Add std dev
    (count, avg, sum, min, max, stddev) => new WindowStats
    {
        Count = count,
        Average = avg,
        Sum = sum,
        Min = min,
        Max = max,
        StandardDeviation = stddev
    })
```

### Example 3: Tumbling Windows (Non-Overlapping)

```csharp
// 5-second windows with no overlap
var windowSize = TimeSpan.FromSeconds(5).Ticks;

inputStream
    .TumblingWindowLifetime(windowSize)
    .Aggregate(...)
```

### Example 4: Session Windows

```csharp
// Windows based on gaps in activity
var timeout = TimeSpan.FromSeconds(30).Ticks;

inputStream
    .SessionWindowLifetime(timeout)
    .Aggregate(...)
```

### Example 5: Threshold Alerting

```csharp
async.Subscribe(new ComplexQueryObserver(this.partitionId));

// Add alerting observer
async.Where(e => e.IsStart && e.Payload.Average > 50_000_000)
    .Subscribe(e => 
    {
        Console.WriteLine($"??  ALERT: High memory usage: {e.Payload.Average:N0} bytes");
    });
```

### Example 6: Percentile Calculations

```csharp
.Aggregate(
    w => w.Count(),
    w => w.Average(e => (double)e),
    w => w.Percentile(50, e => (double)e),  // Median
    w => w.Percentile(95, e => (double)e),  // 95th percentile
    w => w.Percentile(99, e => (double)e),  // 99th percentile
    (count, avg, p50, p95, p99) => new PercentileStats
    {
        Count = count,
        Average = avg,
        MedianP50 = p50,
        P95 = p95,
        P99 = p99
    })
```

## Performance Characteristics

### Memory Usage

- **Window Buffer**: ~O(events per window)
- **Aggregate State**: O(1) for each aggregation
- **Total**: O(window_size / slide_size * events_per_second)

**Example**:
- 5-second windows, 1-second slides
- 10 events/second
- ~5 active windows × 50 events/window = 250 events buffered
- Plus aggregate state (~50 bytes per window)
- Total: ~20 KB of state

### CPU Usage

- **Per Event**: O(number of active windows)
- **Per Aggregation**: O(1) amortized
- **Example**: 5 active windows × 5 aggregations = 25 operations per event

### Checkpoint Size

- Includes all window buffers
- All aggregate states
- Metadata and structure
- **Typical**: 10-100 KB for moderate throughput

## Advanced Patterns

### Pattern 1: Multi-Level Aggregations

```csharp
// First level: Per-second stats
var secondStats = inputStream
    .TumblingWindowLifetime(TimeSpan.FromSeconds(1).Ticks)
    .Aggregate(...);

// Second level: Per-minute stats of the per-second stats
var minuteStats = secondStats
    .TumblingWindowLifetime(TimeSpan.FromMinutes(1).Ticks)
    .Aggregate(...);
```

### Pattern 2: Parallel Queries

```csharp
var inputStream = queryContainer.RegisterInput(...);

// Query 1: Short windows for real-time
var realTime = inputStream
    .HoppingWindowLifetime(TimeSpan.FromSeconds(5).Ticks, TimeSpan.FromSeconds(1).Ticks)
    .Aggregate(...);

// Query 2: Long windows for trends
var trends = inputStream
    .HoppingWindowLifetime(TimeSpan.FromMinutes(5).Ticks, TimeSpan.FromMinutes(1).Ticks)
    .Aggregate(...);

queryContainer.RegisterOutput(realTime).Subscribe(...);
queryContainer.RegisterOutput(trends).Subscribe(...);
```

### Pattern 3: Conditional Aggregation

```csharp
// Only aggregate high-value events
var highValueEvents = inputStream
    .Where(e => e > 50_000_000);

var stats = highValueEvents
    .HoppingWindowLifetime(windowSize, slideSize)
    .Aggregate(...);
```

### Pattern 4: Grouped Aggregation

```csharp
// Aggregate by category
var grouped = inputStream
    .GroupApply(
        e => e % 2,  // Group by even/odd
        g => g.HoppingWindowLifetime(windowSize, slideSize)
              .Aggregate(...),
        (key, stats) => new { IsEven = key == 0, Stats = stats });
```

## Comparison with Other Window Types

| Window Type | Overlapping | Use Case | Memory | Output Frequency |
|-------------|-------------|----------|--------|------------------|
| **Hopping** | Yes | Smooth trends | High | High |
| **Tumbling** | No | Discrete periods | Low | Low |
| **Sliding** | Yes (all) | Real-time | Very High | Very High |
| **Session** | No | Activity-based | Variable | Variable |
| **Snapshot** | N/A | Current state | Very Low | On change |

## Best Practices

### 1. Window Sizing

- **Too Small**: High CPU, frequent outputs
- **Too Large**: High memory, delayed insights
- **Recommended**: Start with 5-60 seconds

### 2. Slide Sizing

- **Small Slides**: Smooth trends, higher CPU
- **Large Slides**: Discrete updates, lower CPU
- **Recommended**: 1/5 to 1/2 of window size

### 3. Aggregation Selection

- Use only needed aggregations
- Consider computational cost
- Some aggregates are incremental (Count, Sum)
- Others require buffering (Percentiles)

### 4. Output Formatting

- Limit console output frequency
- Use sampling for high-volume streams
- Consider batching for efficiency

### 5. Checkpointing

- Match checkpoint interval to window size
- Larger windows = larger checkpoints
- Consider checkpoint storage costs

## Troubleshooting

### Issue: High Memory Usage

**Solutions**:
1. Reduce window size
2. Increase slide size (fewer overlapping windows)
3. Reduce event rate
4. Use tumbling windows instead of hopping

### Issue: Delayed Results

**Causes**:
- Large windows
- Slow aggregations
- Backpressure from observers

**Solutions**:
1. Reduce window size
2. Optimize observer processing
3. Use asynchronous observers

### Issue: Checkpoint Failures

**Causes**:
- Very large window state
- Complex payload types
- Disk space issues

**Solutions**:
1. Reduce window size
2. Simplify payload types
3. Monitor disk space
4. Use compression

## Summary

The complex query demonstrates:

? **Sliding Windows**: Overlapping time windows for smooth analytics
? **Multiple Aggregations**: Count, Average, Sum, Min, Max in one pass
? **Type Safety**: Custom `WindowStats` class
? **Checkpointing**: Full query state persistence
? **Formatted Output**: Beautiful console displays
? **Production Ready**: Error handling, resource management

This pattern is ideal for:
- Real-time monitoring dashboards
- Performance tracking
- Anomaly detection
- Trend analysis
- Capacity planning
- SLA monitoring
