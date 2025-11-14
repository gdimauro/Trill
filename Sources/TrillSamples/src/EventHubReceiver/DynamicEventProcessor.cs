// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Reactive.Subjects;
using Microsoft.StreamProcessing;

namespace EventHubReceiver
{
  /// <summary>
  /// Flexible payload that maintains dynamic values in a serializable format compatible with Trill.
  /// Uses Dictionary&lt;string, object&gt; which can be serialized via ObjectDictionarySurrogate.
  /// </summary>
  public sealed class FlexiblePayload
  {
    private readonly Dictionary<string, object> data = new Dictionary<string, object>();

    /// <summary>
    /// Counter that increments with each event. Preserved across checkpoint/restore cycles.
    /// </summary>
    public long EventCounter { get; set; }

    public FlexiblePayload()
    {
    }

    public FlexiblePayload(IDictionary<string, object> initialData)
    {
      if (initialData != null)
      {
        foreach (var kvp in initialData)
        {
          this.data[kvp.Key] = kvp.Value;
        }
      }
    }

    /// <summary>
    /// Create from ExpandoObject by converting it to Dictionary&lt;string, object&gt;
    /// </summary>
    public static FlexiblePayload FromExpandoObject(ExpandoObject expando)
    {
      var dict = (IDictionary<string, object>)expando;
      return new FlexiblePayload(dict);
    }

    /// <summary>
    /// Set a value in the flexible payload
    /// </summary>
    public void Set(string key, object value)
    {
      this.data[key] = value;
    }

    /// <summary>
    /// Get a strongly-typed value from the flexible payload
    /// </summary>
    public T Get<T>(string key, T defaultValue = default)
    {
      if (this.data.TryGetValue(key, out var value))
      {
        if (value == null)
        {
          return defaultValue;
        }

        if (value is T typedValue)
        {
          return typedValue;
        }

        // Handle numeric conversions (JSON deserialization may change numeric types)
        if (typeof(T).IsPrimitive || typeof(T) == typeof(decimal))
        {
          try
          {
            return (T)Convert.ChangeType(value, typeof(T));
          }
          catch
          {
            return defaultValue;
          }
        }

        // Handle string conversion
        if (typeof(T) == typeof(string))
        {
          return (T)(object)value.ToString();
        }

        return defaultValue;
      }

      return defaultValue;
    }

    /// <summary>
    /// Check if a key exists in the flexible payload
    /// </summary>
    public bool Contains(string key) => this.data.ContainsKey(key);

    /// <summary>
    /// Get all key-value pairs
    /// </summary>
    public IEnumerable<KeyValuePair<string, object>> GetAll() => this.data;

    /// <summary>
    /// Get the underlying dictionary (useful for debugging or advanced scenarios)
    /// </summary>
    public Dictionary<string, object> GetData() => this.data;
  }

  /// <summary>
  /// Result payload for aggregations
  /// </summary>
  public struct AggregationResult
  {
    // Current window statistics
    public ulong Count { get; set; }
    public long Sum { get; set; }
    public double Average { get; set; }
    public long Min { get; set; }
    public long Max { get; set; }
    public long EventCounter { get; set; }
    public string AggregationType { get; set; }

    // Historical/cumulative statistics
    public ulong TotalEventsProcessed { get; set; }
    public long CumulativeSum { get; set; }
    public double CumulativeAverage { get; set; }
    public long HistoricalMin { get; set; }
    public long HistoricalMax { get; set; }
    public ulong WindowNumber { get; set; }
    public double EventsPerSecond { get; set; }
    public long Range { get; set; } // Max - Min
    public double StandardDeviation { get; set; }
  }

  /// <summary>
  /// Dynamic event processor using FlexiblePayload for decoupled aggregation logic.
  /// Demonstrates how to use flexible, serializable objects with Trill.
  /// Uses ObjectDictionarySurrogate to enable checkpoint serialization of Dictionary&lt;string, object&gt;.
  /// </summary>
  public sealed class DynamicEventProcessor : IDisposable
  {
    private static readonly TimeSpan CheckpointInterval = TimeSpan.FromSeconds(10);
    private readonly string checkpointDirectory;
    private readonly string partitionId;
    private readonly DynamicAggregationConfig config;

    private Stopwatch checkpointStopWatch;
    private Subject<StreamEvent<FlexiblePayload>> input;
    private QueryContainer queryContainer;
    private Microsoft.StreamProcessing.Process queryProcess;
    private long lastProcessedSequenceNumber;
    private long eventCounter; // Global counter that persists across checkpoint/restore
    private bool isDisposed;

    /// <summary>
    /// Creates a new FlexiblePayload event processor
    /// </summary>
    /// <param name="partitionId">Identifier for this partition</param>
    /// <param name="checkpointDirectory">Directory where checkpoints will be stored</param>
    /// <param name="config">Configuration for FlexiblePayload aggregations</param>
    public DynamicEventProcessor(
        string partitionId,
        string checkpointDirectory,
        DynamicAggregationConfig config = null)
    {
      this.partitionId = partitionId ?? throw new ArgumentNullException(nameof(partitionId));
      this.checkpointDirectory = checkpointDirectory ?? throw new ArgumentNullException(nameof(checkpointDirectory));
      this.config = config ?? DynamicAggregationConfig.CreateDefault();

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

      // Find the checkpoint with the HIGHEST event counter, not just latest by sequence
      var checkpointFile = GetCheckpointWithHighestCounter();

      if (checkpointFile != null && File.Exists(checkpointFile))
      {
        Console.WriteLine($"Restoring query from checkpoint: {Path.GetFileName(checkpointFile)}");

        // Extract sequence number from filename
        var fileName = Path.GetFileNameWithoutExtension(checkpointFile);
        var parts = fileName.Split('-');
        if (parts.Length > 1 && long.TryParse(parts[2], out long seqNum))
        {
          // Try to restore event counter AND sequence number from metadata file
          var metadataPath = Path.Combine(this.checkpointDirectory, $"{this.partitionId}-{seqNum}.metadata");
          if (File.Exists(metadataPath))
          {
            try
            {
              var metadataText = File.ReadAllText(metadataPath);

              // Check if metadata contains both counter and sequence (format: "counter:sequence")
              if (metadataText.Contains(':'))
              {
                var metadataParts = metadataText.Split(':');
                if (metadataParts.Length == 2 &&
                    long.TryParse(metadataParts[0], out long restoredCounter) &&
                    long.TryParse(metadataParts[1], out long restoredSequence))
                {
                  this.eventCounter = restoredCounter;
                  this.lastProcessedSequenceNumber = restoredSequence;
                  Console.WriteLine($"Restored event counter: {this.eventCounter}, sequence: {this.lastProcessedSequenceNumber}");
                }
              }
              else
              {
                // Old format - just counter
                if (long.TryParse(metadataText, out long restoredCounter))
                {
                  this.eventCounter = restoredCounter;
                  this.lastProcessedSequenceNumber = seqNum;
                  Console.WriteLine($"Restored event counter: {this.eventCounter} (old format)");
                }
              }
            }
            catch (Exception ex)
            {
              Console.WriteLine($"Failed to restore metadata: {ex.Message}");
              this.eventCounter = 0;
              this.lastProcessedSequenceNumber = 0;
            }
          }
        }

        using (var stream = File.OpenRead(checkpointFile))
        {
          CreateQuery();
          try
          {
            this.queryProcess = this.queryContainer.Restore(stream);
          }
          catch (Exception ex)
          {
            Console.WriteLine($"Unable to restore from checkpoint: {ex.Message}");
            Console.WriteLine($"Starting clean");
            CreateQuery();
            this.queryProcess = this.queryContainer.Restore();
            this.eventCounter = 0; // Reset counter on failed restore
            this.lastProcessedSequenceNumber = 0;
          }
        }
      }
      else
      {
        Console.WriteLine($"Clean start of FlexiblePayload query");
        CreateQuery();
        this.queryProcess = this.queryContainer.Restore();
        this.eventCounter = 0; // Start from zero on clean start
        this.lastProcessedSequenceNumber = 0;
      }

      Console.WriteLine($"DynamicEventProcessor initialized. Partition: '{this.partitionId}'");
      Console.WriteLine($"Aggregation Mode: {this.config.AggregationMode}");
      Console.WriteLine($"Starting event counter: {this.eventCounter}, sequence: {this.lastProcessedSequenceNumber}");
    }

    /// <summary>
    /// Process a batch of events
    /// </summary>
    /// <param name="events">Events to process</param>
    public void ProcessEvents(IEnumerable<StreamEvent<FlexiblePayload>> events)
    {
      if (this.isDisposed)
      {
        throw new ObjectDisposedException(nameof(DynamicEventProcessor));
      }

      foreach (var evt in events)
      {
        // Increment and assign counter to payload
        evt.Payload.EventCounter = ++this.eventCounter;
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
    public void ProcessEvent(StreamEvent<FlexiblePayload> evt)
    {
      if (this.isDisposed)
      {
        throw new ObjectDisposedException(nameof(DynamicEventProcessor));
      }

      // Increment and assign counter to payload
      if (evt.Payload != null)
        evt.Payload.EventCounter = ++this.eventCounter;

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
        throw new ObjectDisposedException(nameof(DynamicEventProcessor));
      }

      this.queryProcess?.Flush();
    }

    /// <summary>
    /// Create query with flexible aggregation based on configuration
    /// </summary>
    private void CreateQuery()
    {
      // Create QueryContainer with ObjectDictionarySurrogate to support Dictionary<string, object> serialization
      this.queryContainer = new QueryContainer(new Microsoft.StreamProcessing.Serializer.ObjectDictionarySurrogate());
      this.input = new Subject<StreamEvent<FlexiblePayload>>();

      var inputStream = this.queryContainer.RegisterInput(
          this.input,
          DisorderPolicy.Drop(),
          FlushPolicy.FlushOnPunctuation,
          PeriodicPunctuationPolicy.Time(1));

      var windowSize = this.config.WindowSize.Ticks;
      var slideSize = this.config.SlideSize.Ticks;

      // Create query based on aggregation mode
      IStreamable<Empty, AggregationResult> query;

      switch (this.config.AggregationMode)
      {
        case AggregationMode.SimpleStats:
          query = CreateSimpleStatsQuery(inputStream, windowSize, slideSize);
          break;

        case AggregationMode.CustomFields:
          query = CreateCustomFieldsQuery(inputStream, windowSize, slideSize);
          break;

        case AggregationMode.MultiMetric:
          query = CreateMultiMetricQuery(inputStream, windowSize, slideSize);
          break;

        default:
          query = CreateSimpleStatsQuery(inputStream, windowSize, slideSize);
          break;
      }

      var async = this.queryContainer.RegisterOutput(query);
      async.Subscribe(new DynamicQueryObserver(this.partitionId, this.config));
    }

    /// <summary>
    /// Create a simple statistics query
    /// </summary>
    private IStreamable<Empty, AggregationResult> CreateSimpleStatsQuery(
        IStreamable<Empty, FlexiblePayload> inputStream,
        long windowSize,
        long slideSize)
    {
      return inputStream
          .HoppingWindowLifetime(windowSize, slideSize)
          .Aggregate(
              w => w.Count(),
              w => w.Max(e => e.EventCounter),
              (count, maxCounter) => new AggregationResult
              {
                Count = count,
                EventCounter = maxCounter,
                AggregationType = "SimpleStats"
              });
    }

    private IStreamable<Empty, AggregationResult> CreateCustomFieldsQuery(
            IStreamable<Empty, FlexiblePayload> inputStream,
            long windowSize,
            long slideSize)
    {
      return inputStream
          .HoppingWindowLifetime(windowSize, slideSize)
          .Aggregate(
              w => w.Count(),
              w => w.Sum(e => e.Get<long>("Value", 0L)),
              w => w.Average(e => e.Get<double>("Value", 0.0)),
              w => w.Max(e => e.EventCounter),
              (count, sum, avg, maxCounter) => new AggregationResult
              {
                Count = count,
                Sum = sum,
                Average = avg,
                EventCounter = maxCounter,
                AggregationType = "CustomFields"
              });
    }

    /// <summary>
    /// Create a multi-metric query that tracks various properties
    /// </summary>
    private IStreamable<Empty, AggregationResult> CreateMultiMetricQuery(
        IStreamable<Empty, FlexiblePayload> inputStream,
        long windowSize,
        long slideSize)
    {
      return inputStream
          .HoppingWindowLifetime(windowSize, slideSize)
          .Aggregate(
              w => w.Count(),
              w => w.Sum(e => e.Get<long>("Size", 0L)),
              w => w.Average(e => e.Get<double>("Size", 0.0)),
              w => w.Min(e => e.Get<long>("Size", long.MaxValue)),
              w => w.Max(e => e.Get<long>("Size", long.MinValue)),
              w => w.Max(e => e.EventCounter),
              (count, sum, avg, min, max, maxCounter) => new AggregationResult
              {
                Count = count,
                Sum = sum,
                Average = avg,
                Min = min == long.MaxValue ? 0L : min,
                Max = max == long.MinValue ? 0L : max,
                EventCounter = maxCounter,
                AggregationType = "MultiMetric"
              });
    }

    /// <summary>
    /// Observer for FlexiblePayload query output
    /// </summary>
    private sealed class DynamicQueryObserver : IObserver<StreamEvent<AggregationResult>>
    {
      private readonly string partitionId;
      private readonly DynamicAggregationConfig config;
      private ulong outputCount = 0;

      // Historical tracking
      private ulong totalEventsProcessed = 0;
      private long cumulativeSum = 0;
      private long historicalMin = long.MaxValue;
      private long historicalMax = long.MinValue;
      private long lastWindowEndTime = 0;
      private double runningMeanForStdDev = 0;
      private double runningM2ForStdDev = 0; // For Welford's online algorithm

      public DynamicQueryObserver(string partitionId, DynamicAggregationConfig config)
      {
        this.partitionId = partitionId;
        this.config = config;
      }

      public void OnCompleted()
      {
      }

      public void OnError(Exception error)
      {
        Console.WriteLine($"[{this.partitionId}] Error: {error.Message}");
      }

      public void OnNext(StreamEvent<AggregationResult> value)
      {
        if (value.IsStart)
        {
          this.outputCount++;
          var result = value.Payload;

          // Update historical statistics
          this.totalEventsProcessed += result.Count;
          this.cumulativeSum += result.Sum;

          if (result.Min < this.historicalMin && result.Count > 0)
            this.historicalMin = result.Min;

          if (result.Max > this.historicalMax && result.Count > 0)
            this.historicalMax = result.Max;

          // Calculate cumulative average
          double cumulativeAverage = this.totalEventsProcessed > 0
              ? (double)this.cumulativeSum / this.totalEventsProcessed
              : 0.0;

          // Calculate events per second (based on window duration)
          var windowDurationTicks = value.EndTime - value.StartTime;
          double windowDurationSeconds = windowDurationTicks / (double)TimeSpan.TicksPerSecond;
          double eventsPerSecond = windowDurationSeconds > 0
              ? result.Count / windowDurationSeconds
              : 0.0;

          // Update running statistics for standard deviation (Welford's algorithm)
          // This is a simplified version - for accurate std dev we'd need raw values
          double delta = result.Average - this.runningMeanForStdDev;
          this.runningMeanForStdDev += delta / (double)this.outputCount;
          double delta2 = result.Average - this.runningMeanForStdDev;
          this.runningM2ForStdDev += delta * delta2;

          double variance = this.outputCount > 1 ? this.runningM2ForStdDev / (this.outputCount - 1) : 0;
          double standardDeviation = Math.Sqrt(variance);

          // Calculate range
          long range = result.Count > 0 ? result.Max - result.Min : 0;

          // Format window time range
          var startTime = new DateTime(value.StartTime);
          var endTime = new DateTime(value.EndTime);

          Console.WriteLine($"\n╔════════════════════════════════════════════════════════════════════╗");
          Console.WriteLine($"║ [{this.partitionId}] Window #{this.outputCount}");
          Console.WriteLine($"║ Time: {startTime:HH:mm:ss.fff} - {endTime:HH:mm:ss.fff}");
          Console.WriteLine($"╠════════════════════════════════════════════════════════════════════╣");
          Console.WriteLine($"║ === Current Window Statistics ===");
          Console.WriteLine($"║ Type              : {result.AggregationType}");
          Console.WriteLine($"║ Count             : {result.Count, 15:N0}");
          Console.WriteLine($"║ Event Counter     : {result.EventCounter, 15:N0}");

          if (result.AggregationType != "SimpleStats")
          {
            Console.WriteLine($"║ Sum               : {result.Sum, 15:N0}");
            Console.WriteLine($"║ Average           : {result.Average, 15:N2}");
          }

          if (result.AggregationType == "MultiMetric")
          {
            Console.WriteLine($"║ Minimum           : {result.Min, 15:N0}");
            Console.WriteLine($"║ Maximum           : {result.Max, 15:N0}");
            Console.WriteLine($"║ Range             : {range, 15:N0}");
          }

          Console.WriteLine($"║");
          Console.WriteLine($"║ === Historical Statistics (All Windows) ===");
          Console.WriteLine($"║ Total Windows     : {this.outputCount, 15:N0}");
          Console.WriteLine($"║ Total Events      : {this.totalEventsProcessed, 15:N0}");
          Console.WriteLine($"║ Cumulative Sum    : {this.cumulativeSum, 15:N0}");
          Console.WriteLine($"║ Cumulative Avg    : {cumulativeAverage, 15:N2}");
          Console.WriteLine($"║ Historical Min    : {(this.historicalMin == long.MaxValue ? 0 : this.historicalMin), 15:N0}");
          Console.WriteLine($"║ Historical Max    : {(this.historicalMax == long.MinValue ? 0 : this.historicalMax), 15:N0}");
          Console.WriteLine($"║ Events/Second     : {eventsPerSecond, 15:N2}");
          Console.WriteLine($"║ Std Dev (Avg)     : {standardDeviation, 15:N4}");
          Console.WriteLine($"║ Avg Events/Window : {(double)this.totalEventsProcessed / this.outputCount, 15:N2}");

          Console.WriteLine($"╚════════════════════════════════════════════════════════════════════╝");

          this.lastWindowEndTime = value.EndTime;
        }
      }
    }

    /// <summary>
    /// Take a checkpoint of the current query state
    /// </summary>
    private void TakeCheckpoint()
    {
      Console.WriteLine($"Taking checkpoint at sequence {this.lastProcessedSequenceNumber}, event counter: {this.eventCounter}");

      var checkpointFileName = $"{this.partitionId}-{this.lastProcessedSequenceNumber}.checkpoint";
      var checkpointPath = Path.Combine(this.checkpointDirectory, checkpointFileName);
      var tempPath = checkpointPath + ".tmp";

      var metadataPath = Path.Combine(this.checkpointDirectory, $"{this.partitionId}-{this.lastProcessedSequenceNumber}.metadata");

      try
      {
        // Save Trill query state
        using (var stream = File.Create(tempPath))
        {
          this.queryProcess.Checkpoint(stream);
          stream.Flush();
        }

        // Save metadata (event counter and sequence number) in format: "counter:sequence"
        var metadataContent = $"{this.eventCounter}:{this.lastProcessedSequenceNumber}";
        File.WriteAllText(metadataPath, metadataContent);

        if (File.Exists(checkpointPath))
        {
          File.Delete(checkpointPath);
        }

        File.Move(tempPath, checkpointPath);

        DeleteOlderCheckpoints(checkpointFileName);
        this.checkpointStopWatch.Restart();
        Console.WriteLine($"Checkpoint saved successfully to {Path.GetFullPath(tempPath)}");
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
          catch
          {
          }
        }

        if (File.Exists(metadataPath))
        {
          try
          {
            File.Delete(metadataPath);
          }
          catch
          {
          }
        }
      }
    }

    /// <summary>
    /// Get the checkpoint file with the highest event counter value
    /// </summary>
    private string GetCheckpointWithHighestCounter()
    {
      Console.WriteLine($"[DEBUG] Searching for checkpoints in: {this.checkpointDirectory}");

      if (!Directory.Exists(this.checkpointDirectory))
      {
        Console.WriteLine($"[DEBUG] Checkpoint directory does not exist");
        return null;
      }

      var checkpointFiles = Directory.GetFiles(
          this.checkpointDirectory,
          $"{this.partitionId}-*.checkpoint");

      Console.WriteLine($"[DEBUG] Found {checkpointFiles.Length} checkpoint files");

      if (checkpointFiles.Length == 0)
      {
        return null;
      }

      // Find checkpoint with highest counter value
      string bestCheckpoint = null;
      long highestCounter = -1;

      foreach (var checkpointFile in checkpointFiles)
      {
        var fileName = Path.GetFileNameWithoutExtension(checkpointFile);
        var parts = fileName.Split('-');
        if (parts.Length > 1 && long.TryParse(parts[2], out long seqNum))
        {
          var metadataPath = Path.Combine(this.checkpointDirectory, $"{this.partitionId}-{seqNum}.metadata");
          Console.WriteLine($"[DEBUG] Checking {Path.GetFileName(checkpointFile)}, metadata: {Path.GetFileName(metadataPath)}");

          if (File.Exists(metadataPath))
          {
            try
            {
              var metadataText = File.ReadAllText(metadataPath);
              Console.WriteLine($"[DEBUG]   Metadata content: '{metadataText}'");

              long counter = 0;

              // Check for new format: "counter:sequence"
              if (metadataText.Contains(':'))
              {
                var metadataParts = metadataText.Split(':');
                if (metadataParts.Length == 2 && long.TryParse(metadataParts[0], out counter))
                {
                  Console.WriteLine($"[DEBUG]   Counter value (new format): {counter}");
                }
              }
              else
              {
                // Old format - just counter
                if (long.TryParse(metadataText, out counter))
                {
                  Console.WriteLine($"[DEBUG]   Counter value (old format): {counter}");
                }
              }

              if (counter > highestCounter)
              {
                highestCounter = counter;
                bestCheckpoint = checkpointFile;
                Console.WriteLine($"[DEBUG]   >>> NEW BEST: {Path.GetFileName(checkpointFile)} with counter {counter}");
              }
            }
            catch (Exception ex)
            {
              Console.WriteLine($"[DEBUG]   Error reading metadata: {ex.Message}");
              // Skip checkpoints with invalid metadata
              continue;
            }
          }
          else
          {
            Console.WriteLine($"[DEBUG]   Metadata file not found!");
          }
        }
      }

      Console.WriteLine($"[DEBUG] Best checkpoint: {(bestCheckpoint != null ? Path.GetFileName(bestCheckpoint) : "NONE")} with counter: {highestCounter}");
      return bestCheckpoint;
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
    /// Delete checkpoints with LOWER counter values than current checkpoint.
    /// Keeps checkpoints with equal or higher counters to preserve continuity.
    /// </summary>
    private void DeleteOlderCheckpoints(string currentCheckpointFileName)
    {
      try
      {
        // Get current counter value
        var currentSeqNum = ExtractSequenceNumber(Path.GetFileNameWithoutExtension(currentCheckpointFileName));
        if (!currentSeqNum.HasValue)
        {
          return;
        }

        var currentMetadataPath = Path.Combine(this.checkpointDirectory, $"{this.partitionId}-{currentSeqNum}.metadata");
        if (!File.Exists(currentMetadataPath))
        {
          return;
        }

        long currentCounter = this.eventCounter;
        try
        {
          var metadataText = File.ReadAllText(currentMetadataPath);

          // Parse new format: "counter:sequence" or old format: "counter"
          if (metadataText.Contains(':'))
          {
            var parts = metadataText.Split(':');
            if (parts.Length > 0)
            {
              long.TryParse(parts[0], out currentCounter);
            }
          }
          else
          {
            long.TryParse(metadataText, out currentCounter);
          }
        }
        catch
        {
          return;
        }

        var checkpointFiles = Directory.GetFiles(
            this.checkpointDirectory,
            $"{this.partitionId}-*.checkpoint");

        foreach (var file in checkpointFiles)
        {
          var fileName = Path.GetFileName(file);
          if (fileName == currentCheckpointFileName)
          {
            continue; // Don't delete current checkpoint
          }

          var seqNum = ExtractSequenceNumber(Path.GetFileNameWithoutExtension(file));
          if (!seqNum.HasValue)
          {
            continue;
          }

          var metadataFile = Path.Combine(this.checkpointDirectory, $"{this.partitionId}-{seqNum}.metadata");
          if (File.Exists(metadataFile))
          {
            try
            {
              var metadataText = File.ReadAllText(metadataFile);
              long fileCounter = 0;

              // Parse new format: "counter:sequence" or old format: "counter"
              if (metadataText.Contains(':'))
              {
                var parts = metadataText.Split(':');
                if (parts.Length > 0)
                {
                  long.TryParse(parts[0], out fileCounter);
                }
              }
              else
              {
                long.TryParse(metadataText, out fileCounter);
              }

              // Only delete checkpoints with LOWER counter values
              if (fileCounter < currentCounter)
              {
                File.Delete(file);
                File.Delete(metadataFile);
                Console.WriteLine($"Deleted old checkpoint: {fileName} (counter: {fileCounter})");
              }
              else
              {
                Console.WriteLine($"Keeping checkpoint: {fileName} (counter: {fileCounter} >= {currentCounter})");
              }
            }
            catch (Exception ex)
            {
              Console.WriteLine($"Failed to process checkpoint {fileName}: {ex.Message}");
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
      Console.WriteLine($"DynamicEventProcessor disposed");
    }
  }

  /// <summary>
  /// Configuration for FlexiblePayload aggregation behavior
  /// </summary>
  public sealed class DynamicAggregationConfig
  {
    public AggregationMode AggregationMode { get; set; }

    public TimeSpan WindowSize { get; set; }

    public TimeSpan SlideSize { get; set; }

    /// <summary>
    /// Create default configuration
    /// </summary>
    public static DynamicAggregationConfig CreateDefault()
    {
      return new DynamicAggregationConfig
      {
        AggregationMode = AggregationMode.MultiMetric,
        WindowSize = TimeSpan.FromSeconds(5),
        SlideSize = TimeSpan.FromSeconds(1)
      };
    }

    /// <summary>
    /// Create custom configuration
    /// </summary>
    public static DynamicAggregationConfig Create(
        AggregationMode mode,
        TimeSpan windowSize,
        TimeSpan slideSize)
    {
      return new DynamicAggregationConfig
      {
        AggregationMode = mode,
        WindowSize = windowSize,
        SlideSize = slideSize
      };
    }
  }

  /// <summary>
  /// Available aggregation modes for FlexiblePayload processing
  /// </summary>
  public enum AggregationMode
  {
    /// <summary>Simple event counting</summary>
    SimpleStats,

    /// <summary>Aggregate specific fields from flexible payloads</summary>
    CustomFields,

    /// <summary>Track multiple metrics (count, sum, avg, min, max)</summary>
    MultiMetric
  }
}
