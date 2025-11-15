# Persistent Advanced Samples - Quick Reference Guide

## Quick Start

### Running All Examples
```csharp
TypedRecordAdvancedSample.Run();
```

This runs:
- Examples 1-6: Standard advanced CEP patterns
- **Examples 7-11: Persistent patterns with checkpointing** ?

---

## Example 7: Persistent Stream Join

### What It Does
Demonstrates stream joins with checkpoint/restart capability.

### Code Pattern
```csharp
using (var processor = new TypedEventProcessor<UserActionEnriched>(
    "partition-id",
    checkpointDirectory,
    CreateJoinQuery))
{
    processor.Initialize();  // Restores from checkpoint if available
    
    foreach (var event in events)
    {
        processor.ProcessEvent(event);
    }
    
    processor.Flush();
}  // Auto-checkpoint on dispose
```

### Query Definition
```csharp
IStreamable<Empty, object> CreateJoinQuery(
    IStreamable<Empty, UserActionEnriched> input)
{
    return input
        .TumblingWindowLifetime(TimeSpan.FromSeconds(5).Ticks)
        .Aggregate(
            w => w.Count(),
            w => w.Where(r => r.UserTier == "Platinum").Count(),
            (count, platinumCount) => (object)new {
                TotalActions = count,
                PlatinumActions = platinumCount,
                PlatinumPercentage = platinumCount * 100.0 / Math.Max(count, 1)
            });
}
```

---

## Example 8: Persistent Pattern Detection

### What It Does
Fraud detection with state preservation.

### Query Pattern
```csharp
IStreamable<Empty, object> CreateFraudQuery(
    IStreamable<Empty, FraudAlert> input)
{
    return input
        .Where(alert => alert.AlertLevel != "LOW")
        .Select(alert => (object)new {
            alert.TransactionCount,
            alert.TotalAmount,
            alert.AlertLevel,
            Severity = alert.AlertLevel == "HIGH" ? 10 : 5
        });
}
```

### Use Case
Real-time fraud detection that survives system restarts.

---

## Example 9: Persistent Multi-Stream Correlation

### What It Does
IoT device health monitoring with multi-sensor correlation.

### Query Pattern
```csharp
IStreamable<Empty, object> CreateIoTQuery(
    IStreamable<Empty, DeviceHealthSnapshot> input)
{
    return input
        .Where(s => s.HealthScore < 50)
        .Select(s => (object)new {
            s.DeviceId,
            s.Temperature,
            s.Pressure,
            s.Vibration,
            s.HealthScore,
            AlertType = s.HealthScore < 30 ? "CRITICAL" : "WARNING"
        });
}
```

### Use Case
Continuous device monitoring with state recovery.

---

## Example 10: Persistent Temporal Queries

### What It Does
User session tracking with temporal aggregations.

### Query Pattern
```csharp
IStreamable<Empty, object> CreateSessionQuery(
    IStreamable<Empty, UserSession> input)
{
    return input
        .Where(s => s.ClickCount >= 3)
        .Select(s => (object)new {
            s.ClickCount,
            s.UniquePages,
            s.DurationSeconds,
            EngagementScore = (double)s.ClickCount * s.DurationSeconds / 60.0,
            SessionQuality = s.DurationSeconds > 30 ? "High" : "Low"
        });
}
```

### Use Case
Web analytics with session state preservation.

---

## Example 11: Multi-Phase Checkpointing

### What It Does
Comprehensive checkpoint/restart lifecycle validation.

### Processing Phases
1. **Phase 1**: Initial 3 events ? Checkpoint 1
2. **Phase 2**: Restart + 3 events ? Checkpoint 2
3. **Phase 3**: Restart + 3 events ? Checkpoint 3
4. **Phase 4**: Restart + 2 events ? Verification

### Query Pattern
```csharp
IStreamable<Empty, object> CreateMultiPhaseQuery(
    IStreamable<Empty, SensorAnalytics> input)
{
    return input
        .Select(s => (object)new {
            s.ReadingCount,
            s.AverageValue,
            s.MaxValue,
            s.CriticalCount,
            s.HealthIndicator,
            ProcessPhase = s.ReadingCount <= 3 ? "Phase1" :
                           s.ReadingCount <= 6 ? "Phase2" :
                           s.ReadingCount <= 9 ? "Phase3" : "Phase4"
        });
}
```

### Verification Checklist
- ? 11 total events processed
- ? 3 checkpoints created
- ? 3 successful recoveries
- ? State continuity maintained

---

## TypedEventProcessor API

### Constructor
```csharp
new TypedEventProcessor<TPayload>(
    string partitionId,           // Unique partition identifier
    string checkpointDirectory,   // Directory for checkpoint storage
    Func<IStreamable<Empty, TPayload>, 
         IStreamable<Empty, object>> queryFactory)
```

### Methods

#### Initialize
```csharp
processor.Initialize();
```
- Starts processor
- Restores from latest checkpoint if available
- Falls back to clean start on error

#### ProcessEvent
```csharp
processor.ProcessEvent(StreamEvent<TPayload> evt);
```
- Processes a single event
- Auto-checkpoints every 10 seconds
- Increments sequence number

#### Flush
```csharp
processor.Flush();
```
- Forces output of pending results
- Does not create checkpoint

#### Dispose
```csharp
processor.Dispose();
```
- Creates final checkpoint
- Completes event stream
- Releases resources

---

## Creating Custom Persistent Queries

### Step 1: Define Your Data Type
```csharp
public record MyEvent
{
    public string Id { get; init; }
    public double Value { get; init; }
    public DateTime Timestamp { get; init; }
}
```

### Step 2: Create Query Factory
```csharp
private static IStreamable<Empty, object> CreateMyQuery(
    IStreamable<Empty, MyEvent> input)
{
    return input
        .TumblingWindowLifetime(TimeSpan.FromSeconds(10).Ticks)
        .Aggregate(
            w => w.Count(),
            w => w.Average(e => e.Value),
            (count, avg) => (object)new {
                EventCount = count,
                AverageValue = avg,
                ProcessedAt = DateTime.UtcNow
            });
}
```

### Step 3: Create and Use Processor
```csharp
var checkpointDir = Path.Combine(Path.GetTempPath(), "MyApp", "Checkpoints");

using (var processor = new TypedEventProcessor<MyEvent>(
    "my-partition",
    checkpointDir,
    CreateMyQuery))
{
    processor.Initialize();
    
    // Process your events
    foreach (var evt in myEvents)
    {
        processor.ProcessEvent(evt);
    }
    
    processor.Flush();
}
```

### Step 4: Test Recovery
```csharp
// Simulate crash by creating new processor instance
using (var processor = new TypedEventProcessor<MyEvent>(
    "my-partition",
    checkpointDir,
    CreateMyQuery))
{
    processor.Initialize();  // Restores state!
    
    // Continue processing
    foreach (var evt in moreEvents)
    {
        processor.ProcessEvent(evt);
    }
    
    processor.Flush();
}
```

---

## Checkpoint File Management

### Directory Structure
```
{checkpointDir}/
??? {partition}-{seqnum}.checkpoint    # Query state
??? {partition}-{seqnum}.metadata      # Metadata JSON
??? ...
```

### Checkpoint Metadata
```json
{
  "PartitionId": "my-partition",
  "SequenceNumber": 42,
  "EventCounter": 42,
  "Timestamp": "2024-01-15T10:30:00.000Z",
  "PayloadType": "MyNamespace.MyEvent"
}
```

### Automatic Cleanup
- Old checkpoints deleted automatically
- Only latest checkpoint retained
- Metadata files cleaned up with checkpoints

---

## Configuration

### Required Settings
```csharp
Config.ForceRowBasedExecution = true;  // Required for records/classes
```

### Checkpoint Interval
Default: 10 seconds (in `TypedEventProcessor.cs`)

To customize:
```csharp
private static readonly TimeSpan CheckpointInterval = TimeSpan.FromSeconds(30);
```

---

## Best Practices

### ? DO
1. Use `Config.ForceRowBasedExecution = true` for complex types
2. Dispose processors properly (use `using` statement)
3. Test multi-phase restart scenarios
4. Monitor checkpoint directory size
5. Implement checkpoint retention policies for production
6. Use unique partition IDs for different streams

### ? DON'T
1. Process events after disposing processor
2. Modify checkpoint files manually
3. Delete checkpoints while processor is running
4. Use same partition ID for different data types
5. Ignore checkpoint errors in production

---

## Troubleshooting

### Problem: "Failed to restore from checkpoint"
**Solution**: 
- Check if checkpoint file exists and is not corrupted
- Processor automatically falls back to clean start
- Check console output for detailed error message

### Problem: Checkpoint files growing too large
**Solution**:
- Review query complexity
- Consider checkpoint retention policy
- Implement checkpoint compression
- Monitor state size

### Problem: Restore not working after code changes
**Solution**:
- Type definitions changed - checkpoints incompatible
- Delete old checkpoints and start fresh
- Version your checkpoint schemas for production

### Problem: Events processed multiple times after restart
**Solution**:
- Sequence numbers track processed events
- Check if events are being regenerated with same sequence
- Ensure event source is idempotent

---

## Performance Considerations

### Checkpoint Frequency
- Default: 10 seconds
- Trade-off: Frequency vs. performance
- Adjust based on event rate and state size

### State Size
- Windowed queries accumulate state
- Monitor checkpoint file sizes
- Consider window size vs. checkpoint overhead

### Recovery Time
- Depends on checkpoint file size
- Test restore performance in your scenario
- Consider backup/restore strategies for large states

---

## Example Output

### Checkpoint Creation
```
[my-partition] Taking checkpoint at sequence 42
[my-partition] Checkpoint saved successfully
```

### Restore from Checkpoint
```
[my-partition] Restoring from checkpoint: my-partition-42.checkpoint
[my-partition] Restored from sequence 42
[my-partition] Processor initialized for type: MyEvent
```

### Query Results
```
????????????????????????????????????????????????????????????????
? [my-partition] Output #1
? Input Type: MyEvent
? Time: 10:30:15 ? ?
????????????????????????????????????????????????????????????????
? EventCount           : 42
? AverageValue         : 123.45
? ProcessedAt          : 2024-01-15 10:30:20
????????????????????????????????????????????????????????????????
```

---

## Additional Resources

### Source Files
- `TypedRecordAdvancedSample.cs` - All examples
- `TypedEventProcessor.cs` - Generic processor
- `TypedRecordTypes.cs` - Data type definitions

### Documentation
- `PERSISTENT_ADVANCED_SAMPLES_SUMMARY.md` - Detailed implementation guide
- `TYPED_PROCESSOR_ADVANCED_GUIDE.md` - Advanced processor patterns
- `CHECKPOINTING_GUIDE.md` - Checkpoint/restore details

### Related Examples
- Examples 1-6: Non-persistent advanced patterns
- `TypedRecordSample.cs`: Basic typed record examples
- `CheckpointExample/Program.cs`: Basic checkpointing

---

## Summary

The persistent advanced samples demonstrate production-ready patterns for:
- ? Stream joins with state preservation
- ? Pattern detection with recovery
- ? Multi-stream correlation with checkpointing
- ? Temporal queries with state management
- ? Multi-phase lifecycle management

All powered by the generic `TypedEventProcessor<TPayload>` which handles:
- Automatic checkpointing
- Seamless state recovery
- Metadata tracking
- Error handling
- Resource management

**Ready for production use!** ??
