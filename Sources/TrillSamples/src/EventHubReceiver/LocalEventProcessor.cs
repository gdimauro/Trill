// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reactive.Subjects;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.StreamProcessing;

namespace EventHubReceiver
{
    /// <summary>
    /// Local event processor for Trill query with state - on-premises version without Azure dependencies
    /// </summary>
    public sealed class LocalEventProcessor : IDisposable
    {
        private static readonly TimeSpan CheckpointInterval = TimeSpan.FromSeconds(10);
        private readonly string checkpointDirectory;
        private readonly string partitionId;

        private Stopwatch checkpointStopWatch;
        private Subject<StreamEvent<long>> input;
        private QueryContainer queryContainer;
        private Microsoft.StreamProcessing.Process queryProcess;
        private long lastProcessedSequenceNumber;
        private bool isDisposed;

        /// <summary>
        /// Creates a new local event processor
        /// </summary>
        /// <param name="partitionId">Identifier for this partition</param>
        /// <param name="checkpointDirectory">Directory where checkpoints will be stored</param>
        public LocalEventProcessor(string partitionId, string checkpointDirectory)
        {
            this.partitionId = partitionId ?? throw new ArgumentNullException(nameof(partitionId));
            this.checkpointDirectory = checkpointDirectory ?? throw new ArgumentNullException(nameof(checkpointDirectory));

            if (!Directory.Exists(this.checkpointDirectory))
            {
                Directory.CreateDirectory(this.checkpointDirectory);
            }
        }

        /// <summary>
        /// Initialize the processor and optionally restore from checkpoint
        /// </summary>
        public void Initialize()
        {
            Config.ForceRowBasedExecution = true;

            this.checkpointStopWatch = new Stopwatch();
            this.checkpointStopWatch.Start();

            var checkpointFile = GetLatestCheckpointFile();

            if (checkpointFile != null && File.Exists(checkpointFile))
            {
                Console.WriteLine($"Restoring query from checkpoint: {Path.GetFileName(checkpointFile)}");
                using (var stream = File.OpenRead(checkpointFile))
                {
                    CreateQuery();
                    try
                    {
                        this.queryProcess = this.queryContainer.Restore(stream);

                        // Extract sequence number from filename
                        var fileName = Path.GetFileNameWithoutExtension(checkpointFile);
                        var parts = fileName.Split('-');
                        if (parts.Length > 1 && long.TryParse(parts[1], out long seqNum))
                        {
                            this.lastProcessedSequenceNumber = seqNum;
                        }
                    }
                    catch
                    {
                        Console.WriteLine($"Unable to restore from checkpoint, starting clean");
                        CreateQuery();
                        this.queryProcess = this.queryContainer.Restore();
                    }
                }
            }
            else
            {
                Console.WriteLine($"Clean start of query");
                CreateQuery();
                this.queryProcess = this.queryContainer.Restore();
            }

            Console.WriteLine($"LocalEventProcessor initialized. Partition: '{this.partitionId}'");
        }

        /// <summary>
        /// Process a batch of events
        /// </summary>
        /// <param name="events">Events to process</param>
        public void ProcessEvents(IEnumerable<StreamEvent<long>> events)
        {
            if (this.isDisposed)
            {
                throw new ObjectDisposedException(nameof(LocalEventProcessor));
            }

            foreach (var evt in events)
            {
                this.input.OnNext(evt);
                this.lastProcessedSequenceNumber++;
            }

            if (this.checkpointStopWatch.Elapsed > CheckpointInterval)
            {
                TakeCheckpoint();
            }
        }

        /// <summary>
        /// Process a single event
        /// </summary>
        /// <param name="evt">Event to process</param>
        public void ProcessEvent(StreamEvent<long> evt)
        {
            if (this.isDisposed)
            {
                throw new ObjectDisposedException(nameof(LocalEventProcessor));
            }

            this.input.OnNext(evt);
            this.lastProcessedSequenceNumber++;

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
                throw new ObjectDisposedException(nameof(LocalEventProcessor));
            }

            this.queryProcess?.Flush();
        }

        /// <summary>
        /// Create query and register subscriber
        /// </summary>
        private void CreateQuery()
        {
            this.queryContainer = new QueryContainer();
            this.input = new Subject<StreamEvent<long>>();
            var inputStream = this.queryContainer.RegisterInput(
                this.input,
                DisorderPolicy.Drop(),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(1));

            // Complex query with multiple aggregations and sliding windows
            // Window: 5-second windows sliding every 1 second
            var windowSize = TimeSpan.FromSeconds(5).Ticks;
            var slideSize = TimeSpan.FromSeconds(1).Ticks;

            var query = inputStream
                .HoppingWindowLifetime(windowSize, slideSize)
                .Aggregate(
                    w => w.Count(),
                    w => w.Average(e => (double)e),
                    w => w.Sum(e => e),
                    w => w.Min(e => e),
                    w => w.Max(e => e),
                    (count, avg, sum, min, max) => new WindowStats
                    {
                        Count = count,
                        Average = avg,
                        Sum = sum,
                        Min = min,
                        Max = max
                    });

            var async = this.queryContainer.RegisterOutput(query);
            async.Subscribe(new ComplexQueryObserver(this.partitionId));
        }

        /// <summary>
        /// Data structure for window statistics
        /// </summary>
        private sealed class WindowStats
        {
            public ulong Count { get; set; }
            public double Average { get; set; }
            public long Sum { get; set; }
            public long Min { get; set; }
            public long Max { get; set; }
        }

        /// <summary>
        /// Observer for complex query output with formatted display
        /// </summary>
        private sealed class ComplexQueryObserver : IObserver<StreamEvent<WindowStats>>
        {
            private readonly string partitionId;
            private int outputCount = 0;

            public ComplexQueryObserver(string partitionId)
            {
                this.partitionId = partitionId;
            }

            public void OnCompleted()
            {
            }

            public void OnError(Exception error)
            {
                Console.WriteLine($"[{this.partitionId}] Error: {error.Message}");
            }

            public void OnNext(StreamEvent<WindowStats> value)
            {
                if (value.IsStart)
                {
                    this.outputCount++;
                    var stats = value.Payload;

                    // Format window time range
                    var startTime = new DateTime(value.StartTime);
                    var endTime = new DateTime(value.EndTime);

                    Console.WriteLine($"\n╔════════════════════════════════════════════════════════════╗");
                    Console.WriteLine($"║ [{this.partitionId}] Window #{this.outputCount}");
                    Console.WriteLine($"║ Time: {startTime:HH:mm:ss} - {endTime:HH:mm:ss}");
                    Console.WriteLine($"╠════════════════════════════════════════════════════════════╣");
                    Console.WriteLine($"║ Count    : {stats.Count, 10:N0} events");
                    Console.WriteLine($"║ Average  : {stats.Average, 10:N2} bytes");
                    Console.WriteLine($"║ Sum      : {stats.Sum, 10:N0} bytes");
                    Console.WriteLine($"║ Minimum  : {stats.Min, 10:N0} bytes");
                    Console.WriteLine($"║ Maximum  : {stats.Max, 10:N0} bytes");
                    Console.WriteLine($"╚════════════════════════════════════════════════════════════╝");
                }
            }
        }

        /// <summary>
        /// Take a checkpoint of the current query state
        /// </summary>
        private void TakeCheckpoint()
        {
            Console.WriteLine($"Taking checkpoint at sequence {this.lastProcessedSequenceNumber}");

            var checkpointFileName = $"{this.partitionId}-{this.lastProcessedSequenceNumber}.checkpoint";
            var checkpointPath = Path.Combine(this.checkpointDirectory, checkpointFileName);
            var tempPath = checkpointPath + ".tmp";

            try
            {
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

                DeleteOlderCheckpoints(checkpointFileName);
                this.checkpointStopWatch.Restart();
                Console.WriteLine($"Checkpoint saved successfully");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error taking checkpoint: {ex.Message}");

                if (File.Exists(tempPath))
                {
                    try
                    {
                        File.Delete(tempPath);
                    }
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
                    SequenceNumber = ExtractSequenceNumber(System.IO.Path.GetFileNameWithoutExtension(f))
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
            if (parts.Length > 1 && long.TryParse(parts[1], out long seqNum))
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

                foreach (var file in checkpointFiles)
                {
                    var fileName = Path.GetFileName(file);
                    if (fileName != currentCheckpointFileName)
                    {
                        try
                        {
                            File.Delete(file);
                            Console.WriteLine($"Deleted old checkpoint: {fileName}");
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Failed to delete old checkpoint {fileName}: {ex.Message}");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error during checkpoint cleanup: {ex.Message}");
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
                    Console.WriteLine($"Error taking final checkpoint: {ex.Message}");
                }
            }

            this.input?.OnCompleted();
            this.input?.Dispose();
            this.checkpointStopWatch?.Stop();

            this.isDisposed = true;
            Console.WriteLine($"LocalEventProcessor disposed");
        }
    }
}
