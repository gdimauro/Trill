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
  /// Flexible payload that acts like a dynamic object but works with Trill
  /// </summary>
  public sealed class FlexiblePayload
  {
    private readonly Dictionary<string, object> data = new Dictionary<string, object>();

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

    public static FlexiblePayload FromExpandoObject(ExpandoObject expando)
    {
      return new FlexiblePayload(expando as IDictionary<string, object>);
    }

    public void Set(string key, object value)
    {
      this.data[key] = value;
    }

    public T Get<T>(string key, T defaultValue = default)
    {
      if (this.data.TryGetValue(key, out var value))
      {
        if (value is T typedValue)
        {
          return typedValue;
        }

        try
        {
          return (T)Convert.ChangeType(value, typeof(T));
        }
        catch
        {
          return defaultValue;
        }
      }

      return defaultValue;
    }

    public bool Contains(string key) => this.data.ContainsKey(key);

    public IEnumerable<KeyValuePair<string, object>> GetAll() => this.data;
  }

  /// <summary>
  /// Result payload for aggregations
  /// </summary>
  public sealed class AggregationResult
  {
    public ulong Count { get; set; }
    public long Sum { get; set; }
    public double Average { get; set; }
    public long Min { get; set; }
    public long Max { get; set; }
    public string AggregationType { get; set; }
  }

  /// <summary>
  /// Dynamic event processor using FlexiblePayload for decoupled aggregation logic
  /// Demonstrates how to use flexible objects with Trill
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
    private bool isDisposed;

    /// <summary>
    /// Creates a new dynamic event processor
    /// </summary>
    /// <param name="partitionId">Identifier for this partition</param>
    /// <param name="checkpointDirectory">Directory where checkpoints will be stored</param>
    /// <param name="config">Configuration for dynamic aggregations</param>
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
          catch (Exception ex)
          {
            Console.WriteLine($"Unable to restore from checkpoint: {ex.Message}");
            Console.WriteLine($"Starting clean");
            CreateQuery();
            this.queryProcess = this.queryContainer.Restore();
          }
        }
      }
      else
      {
        Console.WriteLine($"Clean start of dynamic query");
        CreateQuery();
        this.queryProcess = this.queryContainer.Restore();
      }

      Console.WriteLine($"DynamicEventProcessor initialized. Partition: '{this.partitionId}'");
      Console.WriteLine($"Aggregation Mode: {this.config.AggregationMode}");
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
      this.queryContainer = new QueryContainer();
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
          .Aggregate(w => w.Count())
          .Select(count => new AggregationResult
          {
            Count = count,
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
              (count, sum, avg) => new AggregationResult
              {
                Count = count,
                Sum = sum,
                Average = avg,
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
              (count, sum, avg, min, max) => new AggregationResult
              {
                Count = count,
                Sum = sum,
                Average = avg,
                Min = min == long.MaxValue ? 0L : min,
                Max = max == long.MinValue ? 0L : max,
                AggregationType = "MultiMetric"
              });
    }

    /// <summary>
    /// Observer for flexible query output
    /// </summary>
    private sealed class DynamicQueryObserver : IObserver<StreamEvent<AggregationResult>>
    {
      private readonly string partitionId;
      private readonly DynamicAggregationConfig config;
      private int outputCount = 0;

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

          // Format window time range
          var startTime = new DateTime(value.StartTime);
          var endTime = new DateTime(value.EndTime);

          Console.WriteLine($"\n╔════════════════════════════════════════════════════════════╗");
          Console.WriteLine($"║ [{this.partitionId}] Window #{this.outputCount}");
          Console.WriteLine($"║ Time: {startTime:HH:mm:ss.fff} - {endTime:HH:mm:ss.fff}");
          Console.WriteLine($"╠════════════════════════════════════════════════════════════╣");
          Console.WriteLine($"║ Type         : {result.AggregationType}");
          Console.WriteLine($"║ Count        : {result.Count, 10:N0}");

          if (result.AggregationType != "SimpleStats")
          {
            Console.WriteLine($"║ Sum          : {result.Sum, 10:N0}");
            Console.WriteLine($"║ Average      : {result.Average, 10:N2}");
          }

          if (result.AggregationType == "MultiMetric")
          {
            Console.WriteLine($"║ Minimum      : {result.Min, 10:N0}");
            Console.WriteLine($"║ Maximum      : {result.Max, 10:N0}");
          }

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
          catch
          {
          }
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
      Console.WriteLine($"DynamicEventProcessor disposed");
    }
  }

  /// <summary>
  /// Configuration for dynamic aggregation behavior
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
  /// Available aggregation modes for dynamic processing
  /// </summary>
  public enum AggregationMode
  {
    /// <summary>Simple event counting</summary>
    SimpleStats,

    /// <summary>Aggregate specific fields from flexible objects</summary>
    CustomFields,

    /// <summary>Track multiple metrics (count, sum, avg, min, max)</summary>
    MultiMetric
  }
}
