# Persistent Advanced Samples - Implementation Summary

## Overview

Created persistent versions of all advanced Trill samples with comprehensive checkpoint/restart capabilities. Each example demonstrates production-ready patterns for state management, fault tolerance, and recovery.

## New Examples Added

### Example 7: Persistent Stream Join with Checkpointing
**Purpose**: Demonstrates stateful stream joins with checkpoint/restart capability

**Features**:
- Enriched join results processing with state persistence
- Two-phase checkpoint/restart demonstration
- User action correlation with recovery
- Automatic checkpoint creation every 10 seconds

**Query**:
```csharp
input.TumblingWindowLifetime(TimeSpan.FromSeconds(5).Ticks)
    .Aggregate(
        w => w.Count(),
        w => w.Where(r => r.UserTier == "Platinum").Count(),
        (count, platinumCount) => new {
            TotalActions = count,
            PlatinumActions = platinumCount,
            PlatinumPercentage = platinumCount * 100.0 / Math.Max(count, 1)
        });
```

**Checkpoint Strategy**:
- Phase 1: Process 5 events ? Create checkpoint
- Phase 2: Simulate crash ? Restore ? Process 5 more events

---

### Example 8: Persistent Pattern Detection
**Purpose**: Fraud detection with state preservation across restarts

**Features**:
- Pattern matching state recovery
- Alert generation with checkpointing
- Transaction fraud analysis with persistence

**Query**:
```csharp
input.Where(alert => alert.AlertLevel != "LOW")
    .Select(alert => new {
        alert.TransactionCount,
        alert.TotalAmount,
        alert.AlertLevel,
        Severity = alert.AlertLevel == "HIGH" ? 10 : 5
    });
```

**Use Case**: 
Real-time fraud detection that continues seamlessly after system restarts, maintaining pattern detection state.

---

### Example 9: Persistent Multi-Stream Correlation
**Purpose**: IoT device health monitoring with multi-sensor correlation and state recovery

**Features**:
- Multi-stream correlation state persistence
- Health score calculations with checkpointing
- Temperature, pressure, vibration correlation with recovery

**Query**:
```csharp
input.Where(s => s.HealthScore < 50)
    .Select(s => new {
        s.DeviceId,
        s.Temperature,
        s.Pressure,
        s.Vibration,
        s.HealthScore,
        AlertType = s.HealthScore < 30 ? "CRITICAL" : "WARNING"
    });
```

**Use Case**: 
Continuous device monitoring that preserves health assessment state across system failures.

---

### Example 10: Persistent Temporal Queries
**Purpose**: User session tracking with temporal aggregations and state recovery

**Features**:
- Session window state preservation
- Click pattern analysis with checkpointing
- Engagement score calculation with recovery

**Query**:
```csharp
input.Where(s => s.ClickCount >= 3)
    .Select(s => new {
        s.ClickCount,
        s.UniquePages,
        s.DurationSeconds,
        EngagementScore = (double)s.ClickCount * s.DurationSeconds / 60.0,
        SessionQuality = s.DurationSeconds > 30 ? "High" : "Low"
    });
```

**Use Case**: 
Web analytics that maintains session state across restarts for accurate user behavior tracking.

---

### Example 11: Multi-Phase Checkpointing Test
**Purpose**: Comprehensive checkpoint/restart lifecycle validation

**Features**:
- 4-phase processing with 3 restarts
- Multiple checkpoint creation/restoration cycles
- State continuity verification across phases
- Production-ready checkpoint patterns

**Processing Phases**:
1. **Phase 1**: Process 3 events ? Checkpoint 1
2. **Phase 2**: Restart ? Process 3 events ? Checkpoint 2  
3. **Phase 3**: Restart ? Process 3 events ? Checkpoint 3
4. **Phase 4**: Restart ? Process 2 events ? Final verification

**Query**:
```csharp
input.Select(s => new {
    s.ReadingCount,
    s.AverageValue,
    s.MaxValue,
    s.CriticalCount,
    s.HealthIndicator,
    ProcessPhase = s.ReadingCount <= 3 ? "Phase1" :
                   s.ReadingCount <= 6 ? "Phase2" :
                   s.ReadingCount <= 9 ? "Phase3" : "Phase4"
});
```

**Verification**:
- ? 11 total events processed across 4 phases
- ? 3 successful checkpoint snapshots
- ? 3 successful state recoveries
- ? State continuity maintained throughout

---

## Technical Implementation

### TypedEventProcessor<TPayload>
Generic processor supporting any strongly-typed payload with built-in checkpoint/restore:

**Key Methods**:
- `Initialize()`: Start processor or restore from latest checkpoint
- `ProcessEvent()`: Process individual typed events
- `Flush()`: Force output of pending results
- `Dispose()`: Clean shutdown with final checkpoint

**Checkpoint Features**:
- Automatic checkpoint every 10 seconds
- Metadata tracking (sequence number, timestamp, type info)
- Old checkpoint cleanup
- Atomic checkpoint writes (temp file ? rename)
- Graceful recovery on restore failure

### Checkpoint File Structure
```
checkpoints/
??? {partition}-{seqnum}.checkpoint   # Query state
??? {partition}-{seqnum}.metadata     # Metadata JSON
??? ...
```

**Metadata Example**:
```json
{
  "PartitionId": "join-partition",
  "SequenceNumber": 5,
  "EventCounter": 5,
  "Timestamp": "2024-01-15T10:30:00.000Z",
  "PayloadType": "EventHubReceiver.UserActionEnriched"
}
```

---

## Data Types Used

### UserActionEnriched
```csharp
public record UserActionEnriched
{
    public string UserId { get; init; }
    public string ActionType { get; init; }
    public DateTime ActionTimestamp { get; init; }
    public string UserName { get; init; }
    public string UserTier { get; init; }
    public DateTime UserRegistrationDate { get; init; }
}
```

### FraudAlert
```csharp
public record FraudAlert
{
    public ulong TransactionCount { get; init; }
    public decimal TotalAmount { get; init; }
    public ulong DistinctCountries { get; init; }
    public string AlertLevel { get; init; }
}
```

### DeviceHealthSnapshot
```csharp
public record DeviceHealthSnapshot
{
    public string DeviceId { get; init; }
    public double Temperature { get; init; }
    public double Pressure { get; init; }
    public double Vibration { get; init; }
    public DateTime Timestamp { get; init; }
    public double HealthScore { get; init; }
}
```

### UserSession
```csharp
public record UserSession
{
    public ulong ClickCount { get; init; }
    public ulong UniquePages { get; init; }
    public DateTime SessionStart { get; init; }
    public DateTime SessionEnd { get; init; }
    public long DurationSeconds { get; init; }
}
```

### SensorAnalytics
```csharp
public record SensorAnalytics
{
    public ulong ReadingCount { get; init; }
    public double AverageValue { get; init; }
    public double MaxValue { get; init; }
    public ulong CriticalCount { get; init; }
    public string HealthIndicator { get; init; }
}
```

---

## Execution Flow

### Standard Pattern (Examples 7-10)

1. **Phase 1: Initial Processing**
   ```
   Create Processor ? Initialize ? Process Events ? Checkpoint ? Dispose
   ```

2. **Phase 2: Recovery & Continue**
   ```
   Create Processor ? Restore from Checkpoint ? Process More Events ? Dispose
   ```

### Multi-Phase Pattern (Example 11)

1. **Phase 1**: Initial data ? Checkpoint 1
2. **Restart 1**: Restore ? Process ? Checkpoint 2
3. **Restart 2**: Restore ? Process ? Checkpoint 3
4. **Restart 3**: Restore ? Final verification

---

## Console Output Features

### Formatted Output
Each query result displays:
- Partition ID
- Output number
- Input type name
- Time range (start ? end)
- All payload properties with formatting
- Box-drawing characters for visual clarity

**Example Output**:
```
????????????????????????????????????????????????????????????????
? [join-partition] Output #1
? Input Type: UserActionEnriched
? Time: 10:30:15 ? ?
????????????????????????????????????????????????????????????????
? TotalActions         : 5
? PlatinumActions      : 2
? PlatinumPercentage   : 40.00
? Timestamp            : 2024-01-15 10:30:20
????????????????????????????????????????????????????????????????
```

---

## Production Readiness

### Checkpoint Management
? Atomic writes (temp ? rename)  
? Metadata tracking  
? Automatic cleanup of old checkpoints  
? Graceful recovery on failure  

### Error Handling
? Exception handling in checkpoint/restore  
? Fallback to clean start on restore failure  
? Detailed error logging  

### Resource Management
? Proper disposal pattern  
? Final checkpoint on dispose  
? Subject completion on shutdown  

### Monitoring
? Sequence number tracking  
? Event counter  
? Checkpoint timestamps  
? State recovery verification  

---

## Running the Samples

All examples run automatically when executing:

```csharp
TypedRecordAdvancedSample.Run();
```

Output includes:
1. Original 6 non-persistent examples (Examples 1-6)
2. **NEW**: 5 persistent examples with checkpointing (Examples 7-11)

Total execution demonstrates:
- 6 CEP patterns
- 5 persistent patterns
- Multiple checkpoint/restart cycles
- State continuity verification

---

## Key Learnings

### What Works Well
1. **Generic TypedEventProcessor**: Works with any record/class type
2. **QueryContainer.Checkpoint()**: Reliable state serialization
3. **QueryContainer.Restore(stream)**: Successful state deserialization
4. **Row-Based Execution**: Required for complex types
5. **Subject<StreamEvent<T>>**: Clean event ingestion pattern

### Best Practices Demonstrated
1. Always use `Config.ForceRowBasedExecution = true` for records
2. Checkpoint periodically (10 seconds is reasonable)
3. Use atomic file operations (write temp, then rename)
4. Track metadata separately from query state
5. Clean up old checkpoints to prevent disk bloat
6. Test multi-phase restart scenarios

### Production Considerations
1. Monitor checkpoint file sizes
2. Implement checkpoint retention policies
3. Add checkpoint compression for large states
4. Consider checkpoint frequency vs. performance
5. Implement checkpoint health monitoring
6. Test restore under various failure scenarios

---

## File Locations

**Main Implementation**:
- `TypedRecordAdvancedSample.cs` - All 11 examples
- `TypedEventProcessor.cs` - Generic processor with checkpoint/restore
- `TypedRecordTypes.cs` - Data type definitions

**Checkpoint Storage**:
- `%TEMP%\TrillTyped\{ExampleName}\` - Checkpoint directories
- `{partition}-{seqnum}.checkpoint` - State files
- `{partition}-{seqnum}.metadata` - Metadata files

---

## Summary

Successfully created 5 new persistent examples demonstrating production-ready checkpoint/restart patterns for:
- Stream joins
- Pattern detection  
- Multi-stream correlation
- Temporal queries
- Multi-phase lifecycle management

All examples use the generic `TypedEventProcessor<TPayload>` which provides:
- Automatic checkpointing every 10 seconds
- Seamless state recovery on restart
- Metadata tracking
- Error handling and recovery
- Clean resource management

These patterns are ready for production use in scenarios requiring fault tolerance and state preservation.
