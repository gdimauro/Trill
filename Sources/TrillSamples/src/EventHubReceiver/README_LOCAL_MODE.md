# Event Hub Receiver - Local Mode by Default

This project demonstrates Trill event processing with two modes: **Local On-Premises** (default) and **Azure Event Hub**.

## Quick Start

### Run with Default (Local Mode)

```bash
cd TrillSamples/src/EventHubReceiver
dotnet run
```

Press Enter at the menu (or type `1`) to run in local mode.

## Features

### Local On-Premises Mode (Default) ?

- **Zero Configuration**: No setup required, just run!
- **No Azure Dependencies**: Runs completely offline
- **Local Checkpointing**: Automatic state persistence to `%LOCALAPPDATA%\TrillCheckpoints`
- **Simulated Events**: Processes real-time system metrics (process working set)
- **Checkpoint Restoration**: Automatically resumes from last checkpoint
- **Development-Friendly**: Perfect for testing and development

#### What It Does

1. Initializes a `LocalEventProcessor` with local filesystem checkpointing
2. Generates events every second containing the current process working set size
3. Processes events through a Trill query (counts events over time)
4. Takes checkpoints every 10 seconds
5. Displays results in real-time on the console

#### Output Example

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
Processed 20 events (WorkingSet: 46 MB)
[partition-0] Count: [1, 200, 20]
[partition-0] Taking checkpoint at sequence 20
...
```

### Azure Event Hub Mode

- Receives events from Azure Event Hub
- Checkpoints to Azure Blob Storage
- Requires configuration (see below)

## Menu System

The application presents an interactive menu:

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

  3. Exit

Enter your choice (1-3) [default: 1]:
```

## Configuration

### Local Mode

**No configuration required!** Just run the application.

Optional: Change checkpoint directory by modifying `LocalReceiver.cs`:

```csharp
private static readonly string DefaultCheckpointDirectory = "C:\\MyCheckpoints";
```

### Azure Event Hub Mode

Edit `AzureEventHubReceiver.cs` to configure:

```csharp
private const string EventHubConnectionString = "Endpoint=sb://...";
private const string EventHubName = "your-event-hub";
private const string StorageContainerName = "checkpoints";
private const string StorageAccountName = "yourstorageaccount";
private const string StorageAccountKey = "your-storage-key";
```

## Architecture

### Local Mode

```
LocalReceiver
    ??> LocalEventProcessor
            ??> QueryContainer
            ?       ??> Trill Query (Count)
            ??> Subject<StreamEvent<long>>
            ??> Checkpoint Management
                    ??> Local Filesystem
```

### Event Flow

1. **Event Generation**: `LocalReceiver` creates events from process metrics
2. **Processing**: `LocalEventProcessor` receives events
3. **Query Execution**: Events flow through Trill query
4. **Aggregation**: Query counts events over time
5. **Checkpointing**: State saved every 10 seconds
6. **Output**: Results displayed via `SimpleObserver`

## Files

| File | Purpose |
|------|---------|
| **Program.cs** | Main entry point with menu system |
| **LocalReceiver.cs** | Local mode event generator and coordinator |
| **LocalEventProcessor.cs** | Local event processor with checkpointing |
| **AzureEventHubReceiver.cs** | Azure Event Hub receiver (original) |
| **EventProcessor.cs** | Azure event processor implementation |

## Checkpointing

### Local Mode Checkpoints

- **Location**: `%LOCALAPPDATA%\TrillCheckpoints`
- **Format**: `{partitionId}-{sequenceNumber}.checkpoint`
- **Interval**: 10 seconds
- **Retention**: Only latest checkpoint per partition

Example checkpoint files:
```
partition-0-10.checkpoint
partition-0-20.checkpoint
partition-0-30.checkpoint  ? Latest (kept)
```

### Azure Mode Checkpoints

- Stored in Azure Blob Storage
- Container: Configured in `AzureEventHubReceiver`
- Managed by Event Processor Host

## Testing Checkpoint Restoration

1. Start the receiver in local mode
2. Wait for a few checkpoints (observe console output)
3. Stop the application (Ctrl+C)
4. Restart the application
5. Observe: "Restoring from checkpoint: partition-0-XX.checkpoint"

The query state is restored and processing continues!

## Use Cases

### Local Mode

- ? Development and testing
- ? Learning Trill queries
- ? Prototyping new queries
- ? On-premises deployments
- ? Offline scenarios
- ? CI/CD testing

### Azure Mode

- ? Production cloud deployments
- ? High-volume event streams
- ? Multiple partitions
- ? Distributed processing
- ? Cloud-native applications

## Customization

### Change the Query

Modify `LocalEventProcessor.CreateQuery()`:

```csharp
private void CreateQuery()
{
    this.queryContainer = new QueryContainer();
    this.input = new Subject<StreamEvent<long>>();
    var inputStream = this.queryContainer.RegisterInput(
        this.input,
        DisorderPolicy.Drop(),
        FlushPolicy.FlushOnPunctuation,
        PeriodicPunctuationPolicy.Time(1));
    
    // Custom query here!
    var query = inputStream
        .TumblingWindowLifetime(TimeSpan.FromSeconds(5).Ticks)
        .Sum(e => e);
    
    var async = this.queryContainer.RegisterOutput(query);
    async.Subscribe(new SimpleObserver(this.partitionId));
}
```

### Change Event Generation

Modify `LocalReceiver.ProcessSimulatedEventsAsync()`:

```csharp
// Generate different events
var evt = StreamEvent.CreateStart(
    DateTime.UtcNow.Ticks, 
    new MyCustomPayload { ... });
```

### Multiple Partitions

Create multiple processors:

```csharp
var processor1 = new LocalEventProcessor("partition-0", checkpointDir);
var processor2 = new LocalEventProcessor("partition-1", checkpointDir);
processor1.Initialize();
processor2.Initialize();
```

## Troubleshooting

### Checkpoint Issues

If you encounter checkpoint corruption:

1. Stop the application
2. Delete checkpoint files: `%LOCALAPPDATA%\TrillCheckpoints\*.checkpoint`
3. Restart (will start with clean state)

### Performance

- Local mode uses row-based execution (`Config.ForceRowBasedExecution = true`)
- For better performance, set to `false` and ensure payloads are columnar-friendly
- Adjust checkpoint interval in `LocalEventProcessor.cs`

### Memory Usage

Monitor the "WorkingSet" value in the console output:
```
Processed 10 events (WorkingSet: 45 MB)
```

If memory grows unbounded, check:
- Query logic for memory leaks
- Checkpoint intervals (more frequent = less memory)
- Event generation rate

## Comparison with EventHubSender

| Feature | EventHubReceiver | EventHubSender |
|---------|------------------|----------------|
| **Default Mode** | Local | Menu choice |
| **Event Source** | Generated internally | Generated internally |
| **Query** | Count aggregation | N/A (sender only) |
| **Checkpointing** | Local filesystem | Local filesystem |
| **Use Case** | Receiver/processor | Sender/generator |

## Integration

### With EventHubSender (Local Mode)

Both projects can run independently in local mode:

1. Start EventHubReceiver (local mode) - processes local events
2. Start EventHubSender (local mode) - generates and processes events

They share the same checkpoint directory structure but use different partition IDs.

### With Azure Event Hub

1. Configure both projects with same Event Hub
2. EventHubSender sends to Event Hub
3. EventHubReceiver receives from Event Hub
4. Both checkpoint to Azure Storage

## Best Practices

1. **Always use local mode for development** - Fast iteration, no costs
2. **Test checkpoint restoration** - Ensure your query state is properly serialized
3. **Monitor checkpoint file sizes** - Large files indicate state management issues
4. **Use meaningful partition IDs** - Easier debugging and monitoring
5. **Clean up old checkpoints manually** - Only latest is auto-deleted

## Advanced Topics

### Custom Checkpoint Directory

```csharp
var customDir = Path.Combine(Directory.GetCurrentDirectory(), "checkpoints");
var processor = new LocalEventProcessor("partition-0", customDir);
```

### Multiple Queries

Run multiple queries on same events:

```csharp
var query1 = inputStream.Count();
var query2 = inputStream.Sum(e => e);
var query3 = inputStream.Average(e => e);

qc.RegisterOutput(query1).Subscribe(...);
qc.RegisterOutput(query2).Subscribe(...);
qc.RegisterOutput(query3).Subscribe(...);
```

### Custom Observers

```csharp
public sealed class MyCustomObserver : IObserver<StreamEvent<ulong>>
{
    public void OnNext(StreamEvent<ulong> value)
    {
        if (value.IsStart)
        {
            // Send to database, API, etc.
            SaveToDatabase(value.Payload);
        }
    }
    
    public void OnError(Exception error) { /* handle */ }
    public void OnCompleted() { /* cleanup */ }
}
```

## Summary

The EventHubReceiver now defaults to **Local On-Premises Mode**, making it:
- ? Ready to run out of the box
- ?? Perfect for development and testing
- ?? Great for learning Trill
- ?? Easy to customize and extend
- ?? No external dependencies

Just run `dotnet run` and you're processing events with Trill!
