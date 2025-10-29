// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.StreamProcessing;

namespace EventHubSender
{
    /// <summary>
    /// Local event receiver that simulates event processing without Azure dependencies
    /// </summary>
    public sealed class LocalEventReceiver
    {
        private readonly string checkpointDirectory;
        private readonly Dictionary<string, LocalEventProcessor> processors;
        private readonly object lockObject = new object();
        private bool isRunning;
        private CancellationTokenSource cancellationTokenSource;

        /// <summary>
        /// Creates a new local event receiver
        /// </summary>
        /// <param name="checkpointDirectory">Directory where checkpoints will be stored</param>
        public LocalEventReceiver(string checkpointDirectory = null)
        {
            this.checkpointDirectory = checkpointDirectory ?? Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
                "TrillCheckpoints");

            this.processors = new Dictionary<string, LocalEventProcessor>();

            Console.WriteLine($"Local Event Receiver initialized");
            Console.WriteLine($"Checkpoint directory: {this.checkpointDirectory}");
        }

        /// <summary>
        /// Start receiving events
        /// </summary>
        public void Start()
        {
            lock (this.lockObject)
            {
                if (this.isRunning)
                {
                    throw new InvalidOperationException("Receiver is already running");
                }

                this.cancellationTokenSource = new CancellationTokenSource();
                this.isRunning = true;

                Console.WriteLine("Local Event Receiver started");
            }
        }

        /// <summary>
        /// Stop receiving events
        /// </summary>
        public void Stop()
        {
            lock (this.lockObject)
            {
                if (!this.isRunning)
                {
                    return;
                }

                this.cancellationTokenSource?.Cancel();
                this.isRunning = false;

                // Dispose all processors
                foreach (var processor in this.processors.Values)
                {
                    processor.Dispose();
                }
                this.processors.Clear();

                Console.WriteLine("Local Event Receiver stopped");
            }
        }

        /// <summary>
        /// Process an event for a specific partition
        /// </summary>
        /// <param name="partitionId">Partition identifier</param>
        /// <param name="evt">Event to process</param>
        public void ProcessEvent(string partitionId, StreamEvent<long> evt)
        {
            if (!this.isRunning)
            {
                throw new InvalidOperationException("Receiver is not running");
            }

            var processor = GetOrCreateProcessor(partitionId);
            processor.ProcessEvent(evt);
        }

        /// <summary>
        /// Process a batch of events for a specific partition
        /// </summary>
        /// <param name="partitionId">Partition identifier</param>
        /// <param name="events">Events to process</param>
        public void ProcessEvents(string partitionId, IEnumerable<StreamEvent<long>> events)
        {
            if (!this.isRunning)
            {
                throw new InvalidOperationException("Receiver is not running");
            }

            var processor = GetOrCreateProcessor(partitionId);
            processor.ProcessEvents(events);
        }

        /// <summary>
        /// Get or create a processor for a partition
        /// </summary>
        private LocalEventProcessor GetOrCreateProcessor(string partitionId)
        {
            lock (this.lockObject)
            {
                if (!this.processors.TryGetValue(partitionId, out var processor))
                {
                    processor = new LocalEventProcessor(partitionId, this.checkpointDirectory);
                    processor.Initialize();
                    this.processors[partitionId] = processor;
                }
                return processor;
            }
        }

        /// <summary>
        /// Flush all processors
        /// </summary>
        public void FlushAll()
        {
            lock (this.lockObject)
            {
                foreach (var processor in this.processors.Values)
                {
                    processor.Flush();
                }
            }
        }
    }

    /// <summary>
    /// Simple observable adapter for events
    /// </summary>
    internal sealed class EventObservableAdapter : IObservable<StreamEvent<long>>
    {
        private readonly BlockingCollection<StreamEvent<long>> events = new BlockingCollection<StreamEvent<long>>();
        private readonly List<IObserver<StreamEvent<long>>> observers = new List<IObserver<StreamEvent<long>>>();
        private readonly object lockObject = new object();

        public void OnNext(StreamEvent<long> evt)
        {
            lock (this.lockObject)
            {
                foreach (var observer in this.observers)
                {
                    observer.OnNext(evt);
                }
            }
        }

        public void OnCompleted()
        {
            lock (this.lockObject)
            {
                foreach (var observer in this.observers)
                {
                    observer.OnCompleted();
                }
            }
        }

        public IDisposable Subscribe(IObserver<StreamEvent<long>> observer)
        {
            lock (this.lockObject)
            {
                this.observers.Add(observer);
            }
            return new Unsubscriber(this.observers, observer);
        }

        private sealed class Unsubscriber : IDisposable
        {
            private readonly List<IObserver<StreamEvent<long>>> observers;
            private readonly IObserver<StreamEvent<long>> observer;

            public Unsubscriber(List<IObserver<StreamEvent<long>>> observers, IObserver<StreamEvent<long>> observer)
            {
                this.observers = observers;
                this.observer = observer;
            }

            public void Dispose()
            {
                if (this.observer != null && this.observers.Contains(this.observer))
                {
                    this.observers.Remove(this.observer);
                }
            }
        }
    }

    /// <summary>
    /// Local event processor for Trill query with state - on-premises version without Azure dependencies
    /// </summary>
    internal sealed class LocalEventProcessor : IDisposable
    {
        private static readonly TimeSpan CheckpointInterval = TimeSpan.FromSeconds(10);
        private readonly string checkpointDirectory;
        private readonly string partitionId;

        private System.Diagnostics.Stopwatch checkpointStopWatch;
        private EventObservableAdapter input;
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

            this.checkpointStopWatch = new System.Diagnostics.Stopwatch();
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
                        if (parts.Length > 1 && long.TryParse(parts[1], out long seqNum))
                        {
                            this.lastProcessedSequenceNumber = seqNum;
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[{this.partitionId}] Unable to restore: {ex.Message}. Starting clean.");
                        CreateQuery();
                        this.queryProcess = this.queryContainer.Restore();
                    }
                }
            }
            else
            {
                Console.WriteLine($"[{this.partitionId}] Clean start - no checkpoint found");
                CreateQuery();
                this.queryProcess = this.queryContainer.Restore();
            }
        }

        /// <summary>
        /// Process a batch of events
        /// </summary>
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
            this.queryProcess?.Flush();
        }

        /// <summary>
        /// Create query and register subscriber
        /// </summary>
        private void CreateQuery()
        {
            this.queryContainer = new QueryContainer();
            this.input = new EventObservableAdapter();
            var inputStream = this.queryContainer.RegisterInput(
                this.input,
                DisorderPolicy.Drop(),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(1));
            var query = inputStream.AlterEventDuration(StreamEvent.InfinitySyncTime).Count();
            var async = this.queryContainer.RegisterOutput(query);
            async.Subscribe(new SimpleObserver(this.partitionId));
        }

        /// <summary>
        /// Simple observer for output
        /// </summary>
        private sealed class SimpleObserver : IObserver<StreamEvent<ulong>>
        {
            private readonly string partitionId;

            public SimpleObserver(string partitionId)
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

            public void OnNext(StreamEvent<ulong> value)
            {
                if (value.IsStart)
                {
                    Console.WriteLine($"[{this.partitionId}] Count: {value}");
                }
            }
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
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{this.partitionId}] Checkpoint error: {ex.Message}");
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

            var files = Directory.GetFiles(this.checkpointDirectory, $"{this.partitionId}-*.checkpoint");
            if (files.Length == 0)
            {
                return null;
            }

            string latestFile = null;
            long maxSeq = -1;

            foreach (var file in files)
            {
                var fileName = Path.GetFileNameWithoutExtension(file);
                var parts = fileName.Split('-');
                if (parts.Length > 1 && long.TryParse(parts[1], out long seqNum))
                {
                    if (seqNum > maxSeq)
                    {
                        maxSeq = seqNum;
                        latestFile = file;
                    }
                }
            }

            return latestFile;
        }

        /// <summary>
        /// Delete checkpoints older than the specified file
        /// </summary>
        private void DeleteOlderCheckpoints(string currentFileName)
        {
            try
            {
                var files = Directory.GetFiles(this.checkpointDirectory, $"{this.partitionId}-*.checkpoint");
                foreach (var file in files)
                {
                    var fileName = Path.GetFileName(file);
                    if (fileName != currentFileName)
                    {
                        try
                        {
                            File.Delete(file);
                        }
                        catch { }
                    }
                }
            }
            catch { }
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
                catch { }
            }

            this.input?.OnCompleted();
            this.checkpointStopWatch?.Stop();

            this.isDisposed = true;
        }
    }
}
