# EventHubReceiver - Local Mode Implementation Summary

## Overview

The EventHubReceiver project has been updated to run the `LocalEventProcessor` by default, providing a zero-configuration, on-premises event processing experience.

## Changes Made

### ? New Files Created (3 files)

1. **LocalReceiver.cs**
   - Coordinates local event processing
   - Generates simulated events (process working set metrics)
   - Manages LocalEventProcessor lifecycle
   - Handles event generation loop

2. **AzureEventHubReceiver.cs**
   - Extracted original Azure Event Hub receiver logic
   - Maintains all original functionality
   - Clean separation from local mode

3. **README_LOCAL_MODE.md**
   - Comprehensive documentation
   - Usage instructions and examples
   - Customization guide
   - Troubleshooting tips

### ? Modified Files (2 files)

1. **Program.cs**
   - Added interactive menu system
   - Default option is Local Mode (option 1)
   - Can press Enter for quick local start
   - Three options:
     - Local On-Premises Mode (default)
     - Azure Event Hub Mode
     - Exit

2. **EventProcessor.cs**
   - Fixed reference to `StorageConnectionString`
   - Now points to `AzureEventHubReceiver.StorageConnectionString`

## Key Features

### ?? Default Behavior

```bash
dotnet run
# Automatically starts in Local Mode
# Just press Enter to accept default
```

### ?? Menu System

```
??????????????????????????????????????????????????????????????
?      Trill Event Receiver - Mode Selection                ?
??????????????????????????????????????????????????????????????

Choose receiver mode:

  1. Local On-Premises Mode (default)
  2. Azure Event Hub Mode (original)
  3. Exit

Enter your choice (1-3) [default: 1]:
```

### ?? Event Processing

- Generates events every second
- Events contain current process working set size
- Processes through Trill Count query
- Displays results in real-time
- Takes checkpoints every 10 seconds

### ?? Checkpointing

- Location: `%LOCALAPPDATA%\TrillCheckpoints`
- Format: `partition-0-{sequenceNumber}.checkpoint`
- Automatic restoration on restart
- Only latest checkpoint kept

## Sample Output

```
???????????????????????????????????????????????????????????
  Trill Local Event Receiver - On-Premises Mode
???????????????????????????????????????????????????????????

Checkpoint directory: C:\Users\...\AppData\Local\TrillCheckpoints

[partition-0] Clean start - no checkpoint found
LocalEventProcessor initialized. Partition: 'partition-0'
Local Event Receiver started
Processing events from simulated source...
Press Ctrl+C to stop.

Processed 10 events (WorkingSet: 45 MB)
[partition-0] Count: [1, 100, 10]
[partition-0] Taking checkpoint at sequence 10
[partition-0] Checkpoint saved successfully
Processed 20 events (WorkingSet: 46 MB)
[partition-0] Count: [1, 200, 20]
...
```

## Architecture

```
Program.cs (Menu)
    ??> LocalReceiver (default)
    ?       ??> LocalEventProcessor
    ?               ??> QueryContainer + Query
    ?               ??> Subject<StreamEvent<long>>
    ?               ??> Local Filesystem Checkpoints
    ?
    ??> AzureEventHubReceiver (option 2)
            ??> EventProcessorHost
                    ??> EventProcessor
                            ??> QueryContainer + Query
                            ??> Azure Blob Checkpoints
```

## Comparison: Before vs After

| Aspect | Before | After |
|--------|--------|-------|
| **Default Mode** | Azure (needs config) | Local (zero config) |
| **Startup** | Fails without Azure | Works immediately |
| **Configuration** | Required | Optional |
| **Dependencies** | Azure SDK required | No external deps |
| **Use Case** | Production only | Dev/test/prod |

## Benefits

### For Developers
- ? **Instant Start**: No configuration needed
- ?? **Fast Iteration**: Test queries locally
- ?? **Easy Learning**: Understand Trill without Azure
- ?? **Easy Debugging**: All local, easy to inspect

### For Testing
- ? **Repeatable**: Same behavior every run
- ? **Isolated**: No external dependencies
- ? **Fast**: No network latency
- ? **Free**: No Azure costs

### For Production
- ?? **On-Premises**: Deploy without cloud
- ?? **Self-Contained**: No external services
- ??? **Secure**: Data stays local
- ?? **Cost-Effective**: No cloud charges

## Integration with EventHubSender

Both projects now support local mode:

### EventHubSender (Menu-based)
```bash
cd TrillSamples/src/EventHubSender
dotnet run
# Choose option 2 for local mode
```

### EventHubReceiver (Default local)
```bash
cd TrillSamples/src/EventHubReceiver
dotnet run
# Press Enter for local mode (default)
```

Both can run simultaneously, each processing events independently with their own checkpoint directories.

## Testing Checkpoint Restoration

1. Start EventHubReceiver (local mode)
2. Wait for several checkpoints (observe console)
3. Kill the process (Ctrl+C)
4. Restart EventHubReceiver
5. Observe restoration message:
   ```
   [partition-0] Restoring from checkpoint: partition-0-30.checkpoint
   ```
6. Processing resumes from saved state!

## Customization Examples

### Change Event Content

Edit `LocalReceiver.ProcessSimulatedEventsAsync()`:

```csharp
// Instead of working set, use CPU time
var evt = StreamEvent.CreateStart(
    DateTime.UtcNow.Ticks,
    proc.TotalProcessorTime.Ticks);
```

### Change Query

Edit `LocalEventProcessor.CreateQuery()`:

```csharp
// Instead of Count, calculate average
var query = inputStream
    .TumblingWindowLifetime(TimeSpan.FromSeconds(10).Ticks)
    .Average(e => e);
```

### Change Checkpoint Interval

Edit `LocalEventProcessor.cs`:

```csharp
// Change from 10 seconds to 30 seconds
private static readonly TimeSpan CheckpointInterval = 
    TimeSpan.FromSeconds(30);
```

## Build Status

- ? Build successful
- ? No compilation errors
- ? No warnings
- ? All dependencies resolved
- ? Ready to run

## Files Summary

| File | Lines | Purpose |
|------|-------|---------|
| **LocalReceiver.cs** | ~115 | Local event generation & coordination |
| **AzureEventHubReceiver.cs** | ~55 | Azure Event Hub receiver logic |
| **Program.cs** | ~95 | Main entry point with menu |
| **EventProcessor.cs** | 1 line changed | Fixed reference |
| **README_LOCAL_MODE.md** | ~500 | Complete documentation |

## Next Steps

### For Users
1. Run `dotnet run` in EventHubReceiver directory
2. Press Enter to start local mode
3. Observe events being processed
4. Try stopping and restarting to see checkpoint restoration

### For Developers
1. Review `LocalReceiver.cs` to understand event generation
2. Review `LocalEventProcessor.cs` to understand query execution
3. Modify the query to experiment with different aggregations
4. Try custom event payloads and queries

## Conclusion

The EventHubReceiver now provides a **best-in-class developer experience** with:
- ? Zero configuration
- ?? Instant startup
- ?? Clear, educational code
- ?? Easy customization
- ?? No external dependencies

The default local mode makes it perfect for:
- Learning Trill
- Developing queries
- Testing checkpoint/restore
- On-premises deployments
- Offline scenarios

While still maintaining full Azure Event Hub support for production cloud deployments when needed!
