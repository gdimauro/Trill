# Local On-Premises Event Processing

This directory contains code for running Trill event processing both with Azure Event Hubs (original) and in a local on-premises mode without Azure dependencies.

## Files Overview

### New Files (On-Premises Mode)

1. **LocalEventProcessor.cs** (in EventHubReceiver project)
   - Local event processor that mimics Azure Event Processor functionality
   - Handles checkpointing to local filesystem
   - Manages query state and restoration

2. **LocalEventReceiver.cs** (in EventHubSender project)
   - Coordinates multiple local event processors
   - Manages partition-based processing
   - Handles checkpoint directory management

3. **LocalSenderReceiver.cs** (in EventHubSender project)
   - Combines sender and receiver functionality for local mode
   - Simulates event generation and processing locally
   - No Azure dependencies

4. **AzureEventHubSender.cs** (in EventHubSender project)
   - Extracted original Azure Event Hub sender logic
   - Maintains original functionality

### Modified Files

- **Program.cs** (in EventHubSender project)
  - Added menu system to choose between modes
  - Clean separation of Azure and local execution paths

## Features

### Local On-Premises Mode Features

1. **Local Filesystem Checkpointing**
   - Checkpoints stored in: `%LOCALAPPDATA%\TrillCheckpoints`
   - Automatic checkpoint cleanup (keeps only latest)
   - Checkpoint interval: 10 seconds
   - Atomic file writes for reliability

2. **Query State Management**
   - Automatic state restoration from latest checkpoint
   - Clean startup if no checkpoint exists
   - Graceful error handling with fallback to clean state

3. **Event Processing**
   - Processes events using the same Trill query logic
   - Counts events over infinite duration windows
   - Outputs results to console

4. **Partition Support**
   - Multiple partition support
   - Each partition maintains independent state
   - Checkpoint files named by partition ID and sequence number

## Usage

### Running the Application

```bash
cd TrillSamples/src/EventHubSender
dotnet run
```

### Menu Options

1. **Azure Event Hub Mode (original)**
   - Requires Azure Event Hub configuration
   - Connection string and Event Hub name must be configured in `AzureEventHubSender.cs`
   - Uses Azure Blob Storage for checkpoints

2. **Local On-Premises Mode (new)**
   - No configuration required
   - Runs entirely locally
   - Checkpoints to local filesystem
   - Self-contained event generation and processing

3. **Exit**
   - Clean shutdown

## Architecture

### Local Mode Architecture

```
LocalSenderReceiver
    ??> LocalEventReceiver
            ??> LocalEventProcessor (per partition)
                    ??> QueryContainer
                    ?       ??> Trill Query
                    ??> Checkpoint Management
```

### Data Flow

1. **Event Generation**: LocalSenderReceiver generates events (process working set size)
2. **Event Processing**: LocalEventReceiver routes to appropriate LocalEventProcessor
3. **Query Execution**: Trill query counts events over time
4. **Checkpointing**: Periodic state saves to filesystem
5. **State Restoration**: Automatic recovery on restart

## Checkpoint File Format

Checkpoint files are named: `{partitionId}-{sequenceNumber}.checkpoint`

Example:
```
partition-0-1.checkpoint
partition-0-2.checkpoint
partition-0-3.checkpoint  <- Latest (kept)
```

Only the latest checkpoint per partition is retained.

## Key Differences Between Modes

| Feature | Azure Mode | Local Mode |
|---------|------------|------------|
| Dependencies | Azure SDK required | No Azure dependencies |
| Checkpoint Storage | Azure Blob Storage | Local filesystem |
| Event Source | Azure Event Hub | In-process generation |
| Configuration | Connection strings required | Zero configuration |
| Use Case | Production cloud deployment | Development/testing/on-prem |

## Extending the Local Mode

### Adding Custom Event Sources

Modify `LocalSenderReceiver.SendLocalEventsAsync()` to accept events from:
- File system
- TCP/UDP sockets
- Message queues (RabbitMQ, MSMQ, etc.)
- Databases
- Other local sources

### Changing the Query

Modify `LocalEventProcessor.CreateQuery()` to implement different Trill queries:
- Windowed aggregations
- Pattern matching
- Joins
- Temporal operations

### Multiple Partitions

```csharp
// Process events for different partitions
receiver.ProcessEvent("partition-0", event1);
receiver.ProcessEvent("partition-1", event2);
receiver.ProcessEvent("partition-2", event3);
```

## Error Handling

The local mode includes comprehensive error handling:

1. **Checkpoint Errors**: Falls back to clean state if restoration fails
2. **Processing Errors**: Logged but doesn't stop processing
3. **File System Errors**: Graceful handling with appropriate logging
4. **Graceful Shutdown**: Final checkpoint taken on disposal

## Performance Considerations

- Checkpoint interval is configurable (default: 10 seconds)
- File I/O is minimized through buffering
- Atomic file writes prevent corruption
- Old checkpoints are automatically deleted

## Troubleshooting

### Checkpoint Directory Access

If you encounter file access issues, the checkpoint directory can be changed:

```csharp
var receiver = new LocalEventReceiver("C:\\MyCheckpoints");
```

### State Corruption

If checkpoints become corrupted:

1. Stop the application
2. Delete files in checkpoint directory
3. Restart (will start with clean state)

### Memory Usage

The local mode uses the same Trill engine as Azure mode:
- Set `Config.ForceRowBasedExecution = true` for lower memory usage
- Adjust checkpoint intervals for memory/performance tradeoff

## Future Enhancements

Possible improvements for the local mode:

1. **Configuration File**: JSON/XML config for checkpoint paths, intervals, etc.
2. **Multiple Queries**: Support multiple simultaneous queries
3. **Monitoring**: Add metrics and health endpoints
4. **Web UI**: Dashboard for query status and results
5. **Compression**: Compress checkpoint files to save space
6. **Encryption**: Encrypt checkpoint data at rest
7. **Replication**: Replicate checkpoints to multiple locations
