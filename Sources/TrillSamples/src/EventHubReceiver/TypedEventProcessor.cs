// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
#pragma warning disable SA1008 // Opening parenthesis must be spaced correctly
#pragma warning disable SA1013 // Closing brace must be followed by a space
#pragma warning disable SA1121 // Use built-in type alias
#pragma warning disable SA1001 // Commas must be spaced correctly
#pragma warning disable SA1028 // Code must not contain trailing whitespace
#pragma warning disable SA1021 // Negative signs must be spaced correctly

using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reactive.Subjects;
using Microsoft.StreamProcessing;

namespace EventHubReceiver
{
    /// <summary>
    /// Generic event processor for strongly-typed C# records and classes
    /// Supports checkpoint/restore with any strongly-typed event
    /// </summary>
    /// <typeparam name="TPayload">The strongly-typed payload type</typeparam>
    public sealed class TypedEventProcessor<TPayload> : IDisposable
    {
        private static readonly TimeSpan CheckpointInterval = TimeSpan.FromSeconds(10);

        private readonly string checkpointDirectory;
        private readonly string partitionId;
        private readonly Func<IStreamable<Empty, TPayload>, IStreamable<Empty, object>> queryFactory;

        private Stopwatch checkpointStopWatch;
        private Subject<StreamEvent<TPayload>> input;
        private QueryContainer queryContainer;
        private Microsoft.StreamProcessing.Process queryProcess;
        private long lastProcessedSequenceNumber;
        private long eventCounter;
        private bool isDisposed;

        /// <summary>
        /// Creates a typed event processor
        /// </summary>
        /// <param name="partitionId">Partition identifier</param>
        /// <param name="checkpointDirectory">Directory for checkpoint storage</param>
        /// <param name="queryFactory">Factory method to create the query</param>
        public TypedEventProcessor(
            string partitionId,
            string checkpointDirectory,
            Func<IStreamable<Empty, TPayload>, IStreamable<Empty, object>> queryFactory)
        {
            this.partitionId = partitionId ?? throw new ArgumentNullException(nameof(partitionId));
            this.checkpointDirectory = checkpointDirectory ?? throw new ArgumentNullException(nameof(checkpointDirectory));
            this.queryFactory = queryFactory ?? throw new ArgumentNullException(nameof(queryFactory));

            if (!Directory.Exists(this.checkpointDirectory))
            {
                Directory.CreateDirectory(this.checkpointDirectory);
            }
        }

        /// <summary>
        /// Initialize the processor, optionally restoring from checkpoint
        /// </summary>
        public void Initialize()
        {
            Config.ForceRowBasedExecution = true;

            this.checkpointStopWatch = new Stopwatch();
            this.checkpointStopWatch.Start();

            var checkpointFile = GetLatestCheckpointFile();

            if (checkpointFile != null && File.Exists(checkpointFile))
            {
                Console.WriteLine($"[{this.partitionId}] Restoring from checkpoint: {Path.GetFileName(checkpointFile)}");
                using (var stream = File.OpenRead(checkpointFile))
                {
                    CreateQuery();
                    try
                    {
                        this.queryProcess = this.queryContainer.Restore(stream);

                        // Extract sequence number from filename
                        var fileName = Path.GetFileNameWithoutExtension(checkpointFile);
                        var parts = fileName.Split('-');
                        if (parts.Length > 1 && long.TryParse(parts.Last(), out long seqNum))
                        {
                            this.lastProcessedSequenceNumber = seqNum;
                        }

                        Console.WriteLine($"[{this.partitionId}] Restored from sequence {this.lastProcessedSequenceNumber}");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[{this.partitionId}] Failed to restore: {ex.Message}. Starting clean.");
                        CreateQuery();
                        this.queryProcess = this.queryContainer.Restore();
                    }
                }
            }
            else
            {
                Console.WriteLine($"[{this.partitionId}] Starting clean (no checkpoint found)");
                CreateQuery();
                this.queryProcess = this.queryContainer.Restore();
            }

            Console.WriteLine($"[{this.partitionId}] Processor initialized for type: {typeof(TPayload).Name}");
        }

        /// <summary>
        /// Process a single event
        /// </summary>
        public void ProcessEvent(StreamEvent<TPayload> evt)
        {
            if (this.isDisposed)
            {
                throw new ObjectDisposedException(nameof(TypedEventProcessor<TPayload>));
            }

            this.input.OnNext(evt);
            this.lastProcessedSequenceNumber++;
            this.eventCounter++;

            if (this.checkpointStopWatch.Elapsed > CheckpointInterval)
            {
                TakeCheckpoint();
            }
        }

        /// <summary>
        /// Flush any pending output
        /// </summary>
        public void Flush()
        {
            if (this.isDisposed)
            {
                throw new ObjectDisposedException(nameof(TypedEventProcessor<TPayload>));
            }

            this.queryProcess?.Flush();
        }

        /// <summary>
        /// Create query and register subscriber
        /// </summary>
        private void CreateQuery()
        {
            this.queryContainer = new QueryContainer();
            this.input = new Subject<StreamEvent<TPayload>>();
            
            var inputStream = this.queryContainer.RegisterInput(
                this.input,
                DisorderPolicy.Drop(),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time((ulong)TimeSpan.FromSeconds(1).Ticks));

            var query = this.queryFactory(inputStream);

            var async = this.queryContainer.RegisterOutput(query);
            async.Subscribe(new TypedQueryObserver<TPayload>(this.partitionId));
        }

        /// <summary>
        /// Take a checkpoint of the current query state
        /// </summary>
        private void TakeCheckpoint()
        {
            Console.WriteLine($"[{this.partitionId}] Taking checkpoint at sequence {this.lastProcessedSequenceNumber}");

            var checkpointFileName = $"{this.partitionId}-{this.lastProcessedSequenceNumber}.checkpoint";
            var checkpointPath = Path.Combine(this.checkpointDirectory, checkpointFileName);
            var tempPath = checkpointPath + ".tmp";

            // Save metadata
            var metadataPath = Path.Combine(
                this.checkpointDirectory,
                $"{this.partitionId}-{this.lastProcessedSequenceNumber}.metadata");

            try
            {
                // Checkpoint query state
                using (var stream = File.Create(tempPath))
                {
                    this.queryProcess.Checkpoint(stream);
                    stream.Flush();
                }

                if (File.Exists(checkpointPath))
                {
                    File.Delete(checkpointPath);
                }

                File.Move(tempPath, checkpointPath);

                // Save metadata
                File.WriteAllText(metadataPath, $@"{{
  ""PartitionId"": ""{this.partitionId}"",
  ""SequenceNumber"": {this.lastProcessedSequenceNumber},
  ""EventCounter"": {this.eventCounter},
  ""Timestamp"": ""{DateTime.UtcNow:O}"",
  ""PayloadType"": ""{typeof(TPayload).FullName}""
}}");

                DeleteOlderCheckpoints(checkpointFileName);
                this.checkpointStopWatch.Restart();

                Console.WriteLine($"[{this.partitionId}] Checkpoint saved successfully");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{this.partitionId}] Error taking checkpoint: {ex.Message}");

                if (File.Exists(tempPath))
                {
                    try { File.Delete(tempPath); }
                    catch { }
                }
            }
        }

        /// <summary>
        /// Get the latest checkpoint file for this partition
        /// </summary>
        private string GetLatestCheckpointFile()
        {
            if (!Directory.Exists(this.checkpointDirectory))
            {
                return null;
            }

            var checkpointFiles = Directory.GetFiles(
                this.checkpointDirectory,
                $"{this.partitionId}-*.checkpoint");

            if (checkpointFiles.Length == 0)
            {
                return null;
            }

            return checkpointFiles
                .Select(f => new
                {
                    Path = f,
                    SequenceNumber = ExtractSequenceNumber(Path.GetFileNameWithoutExtension(f))
                })
                .Where(x => x.SequenceNumber.HasValue)
                .OrderByDescending(x => x.SequenceNumber.Value)
                .Select(x => x.Path)
                .FirstOrDefault();
        }

        /// <summary>
        /// Extract sequence number from checkpoint filename
        /// </summary>
        private long? ExtractSequenceNumber(string fileName)
        {
            var parts = fileName.Split('-');
            if (parts.Length > 1 && long.TryParse(parts.Last(), out long seqNum))
            {
                return seqNum;
            }

            return null;
        }

        /// <summary>
        /// Delete checkpoints older than the specified file
        /// </summary>
        private void DeleteOlderCheckpoints(string currentCheckpointFileName)
        {
            try
            {
                var checkpointFiles = Directory.GetFiles(
                    this.checkpointDirectory,
                    $"{this.partitionId}-*.checkpoint");

                var metadataFiles = Directory.GetFiles(
                    this.checkpointDirectory,
                    $"{this.partitionId}-*.metadata");

                foreach (var file in checkpointFiles.Concat(metadataFiles))
                {
                    var fileName = Path.GetFileName(file);
                    if (!fileName.StartsWith(Path.GetFileNameWithoutExtension(currentCheckpointFileName)))
                    {
                        try
                        {
                            File.Delete(file);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[{this.partitionId}] Failed to delete old file {fileName}: {ex.Message}");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{this.partitionId}] Error during checkpoint cleanup: {ex.Message}");
            }
        }

        /// <summary>
        /// Dispose of resources
        /// </summary>
        public void Dispose()
        {
            if (this.isDisposed)
            {
                return;
            }

            if (this.queryProcess != null)
            {
                try
                {
                    TakeCheckpoint();
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[{this.partitionId}] Error taking final checkpoint: {ex.Message}");
                }
            }

            this.input?.OnCompleted();
            this.input?.Dispose();
            this.checkpointStopWatch?.Stop();

            this.isDisposed = true;
            Console.WriteLine($"[{this.partitionId}] TypedEventProcessor<{typeof(TPayload).Name}> disposed");
        }
    }

    /// <summary>
    /// Observer for typed query results
    /// </summary>
    internal sealed class TypedQueryObserver<TInputType> : IObserver<StreamEvent<object>>
    {
        private readonly string partitionId;
        private ulong outputCount = 0;

        public TypedQueryObserver(string partitionId)
        {
            this.partitionId = partitionId;
        }

        public void OnCompleted()
        {
            Console.WriteLine($"[{this.partitionId}] Query completed. Total outputs: {this.outputCount}");
        }

        public void OnError(Exception error)
        {
            Console.WriteLine($"[{this.partitionId}] Error: {error.Message}");
        }

        public void OnNext(StreamEvent<object> value)
        {
            if (value.IsStart)
            {
                this.outputCount++;

                var startTime = new DateTime(value.StartTime);
                var endTime = value.EndTime == StreamEvent.InfinitySyncTime
                    ? "∞"
                    : new DateTime(value.EndTime).ToString("HH:mm:ss");

                Console.WriteLine($"╔══════════════════════════════════════════════════════════════╗");
                Console.WriteLine($"║ [{this.partitionId}] Output #{this.outputCount}");
                Console.WriteLine($"║ Input Type: {typeof(TInputType).Name}");
                Console.WriteLine($"║ Time: {startTime:HH:mm:ss} → {endTime}");
                Console.WriteLine($"╠══════════════════════════════════════════════════════════════╣");

                // Display payload properties using reflection
                var payload = value.Payload;
                var props = payload.GetType().GetProperties();

                foreach (var prop in props.OrderBy(p => p.Name))
                {
                    var val = prop.GetValue(payload);
                    var formatted = FormatValue(val);
                    Console.WriteLine($"║ {prop.Name,-20} : {formatted}");
                }

                Console.WriteLine($"╚══════════════════════════════════════════════════════════════╝");
                Console.WriteLine();
            }
        }

        private string FormatValue(object value)
        {
            if (value == null) return "null";

            return value switch
            {
                double d => $"{d:F2}",
                decimal m => $"{m:F2}",
                ulong ul => $"{ul:N0}",
                long l => $"{l:N0}",
                DateTime dt => dt.ToString("yyyy-MM-dd HH:mm:ss"),
                _ => value.ToString()
            };
        }
    }
}
