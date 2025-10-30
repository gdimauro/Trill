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
using Microsoft.StreamProcessing;

namespace EventHubReceiver.Kql
{
    /// <summary>
    /// Event processor that uses KQL-defined schemas for dynamic typing
    /// Combines the flexibility of KQL schemas with Trill's checkpointing capabilities
    /// </summary>
    public sealed class KqlEventProcessor : IDisposable
    {
        private static readonly TimeSpan CheckpointInterval = TimeSpan.FromSeconds(10);
        private readonly string checkpointDirectory;
        private readonly string partitionId;
        private readonly KqlTableSchema schema;
        private readonly KqlQueryConfig queryConfig;

        private Stopwatch checkpointStopWatch;
        private Subject<StreamEvent<KqlDynamicRecord>> input;
        private QueryContainer queryContainer;
        private Microsoft.StreamProcessing.Process queryProcess;
        private long lastProcessedSequenceNumber;
        private long eventCounter;
        private bool isDisposed;

        /// <summary>
        /// Creates a new KQL event processor
        /// </summary>
        /// <param name="partitionId">Identifier for this partition</param>
        /// <param name="checkpointDirectory">Directory where checkpoints will be stored</param>
        /// <param name="schema">KQL table schema defining the structure</param>
        /// <param name="queryConfig">Query configuration</param>
        public KqlEventProcessor(
            string partitionId,
            string checkpointDirectory,
            KqlTableSchema schema,
            KqlQueryConfig queryConfig = null)
        {
            this.partitionId = partitionId ?? throw new ArgumentNullException(nameof(partitionId));
            this.checkpointDirectory = checkpointDirectory ?? throw new ArgumentNullException(nameof(checkpointDirectory));
            this.schema = schema ?? throw new ArgumentNullException(nameof(schema));
            this.queryConfig = queryConfig ?? KqlQueryConfig.CreateDefault();

            if (!Directory.Exists(this.checkpointDirectory))
            {
                Directory.CreateDirectory(this.checkpointDirectory);
            }

            ValidateSchema();
        }

        /// <summary>
        /// Validate that the schema has required fields for aggregation
        /// </summary>
        private void ValidateSchema()
        {
            if (!string.IsNullOrEmpty(queryConfig.AggregationField))
            {
                var column = schema.GetColumn(queryConfig.AggregationField);
                if (column == null)
                {
                    throw new ArgumentException(
                        $"Aggregation field '{queryConfig.AggregationField}' not found in schema '{schema.TableName}'");
                }

                // Verify field is numeric for aggregation
                if (column.DataType != KqlDataType.Int &&
                    column.DataType != KqlDataType.Long &&
                    column.DataType != KqlDataType.Real &&
                    column.DataType != KqlDataType.Decimal)
                {
                    throw new ArgumentException(
                        $"Aggregation field '{queryConfig.AggregationField}' must be numeric type, but is {column.DataType}");
                }
            }
        }

        /// <summary>
        /// Initialize the processor and optionally restore from checkpoint
        /// </summary>
        public void Initialize()
        {
            Config.ForceRowBasedExecution = true;

            checkpointStopWatch = new Stopwatch();
            checkpointStopWatch.Start();

            var checkpointFile = GetCheckpointWithHighestCounter();

            if (checkpointFile != null && File.Exists(checkpointFile))
            {
                Console.WriteLine($"Restoring KQL query from checkpoint: {Path.GetFileName(checkpointFile)}");

                var fileName = Path.GetFileNameWithoutExtension(checkpointFile);
                var parts = fileName.Split('-');
                if (parts.Length > 2 && long.TryParse(parts[2], out long seqNum))
                {
                    var metadataPath = Path.Combine(checkpointDirectory, $"{partitionId}-{seqNum}.metadata");
                    if (File.Exists(metadataPath))
                    {
                        try
                        {
                            var metadataText = File.ReadAllText(metadataPath);
                            if (metadataText.Contains(':'))
                            {
                                var metadataParts = metadataText.Split(':');
                                if (metadataParts.Length == 2 &&
                                    long.TryParse(metadataParts[0], out long restoredCounter) &&
                                    long.TryParse(metadataParts[1], out long restoredSequence))
                                {
                                    eventCounter = restoredCounter;
                                    lastProcessedSequenceNumber = restoredSequence;
                                    Console.WriteLine($"Restored event counter: {eventCounter}, sequence: {lastProcessedSequenceNumber}");
                                }
                            }
                            else
                            {
                                if (long.TryParse(metadataText, out long restoredCounter))
                                {
                                    eventCounter = restoredCounter;
                                    lastProcessedSequenceNumber = seqNum;
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Failed to restore metadata: {ex.Message}");
                            eventCounter = 0;
                            lastProcessedSequenceNumber = 0;
                        }
                    }
                }

                using (var stream = File.OpenRead(checkpointFile))
                {
                    CreateQuery();
                    try
                    {
                        queryProcess = queryContainer.Restore(stream);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Unable to restore from checkpoint: {ex.Message}");
                        Console.WriteLine($"Starting clean");
                        CreateQuery();
                        queryProcess = queryContainer.Restore();
                        eventCounter = 0;
                        lastProcessedSequenceNumber = 0;
                    }
                }
            }
            else
            {
                Console.WriteLine($"Clean start of KQL query for schema: {schema.TableName}");
                CreateQuery();
                queryProcess = queryContainer.Restore();
                eventCounter = 0;
                lastProcessedSequenceNumber = 0;
            }

            Console.WriteLine($"KqlEventProcessor initialized. Partition: '{partitionId}'");
            Console.WriteLine($"Schema: {schema}");
            Console.WriteLine($"Query Type: {queryConfig.QueryType}");
            Console.WriteLine($"Starting event counter: {eventCounter}, sequence: {lastProcessedSequenceNumber}");
        }

        /// <summary>
        /// Process a batch of events
        /// </summary>
        public void ProcessEvents(IEnumerable<StreamEvent<KqlDynamicRecord>> events)
        {
            if (isDisposed)
            {
                throw new ObjectDisposedException(nameof(KqlEventProcessor));
            }

            foreach (var evt in events)
            {
                evt.Payload.EventCounter = ++eventCounter;
                input.OnNext(evt);
                lastProcessedSequenceNumber++;
            }

            if (checkpointStopWatch.Elapsed > CheckpointInterval)
            {
                TakeCheckpoint();
            }
        }

        /// <summary>
        /// Process a single event
        /// </summary>
        public void ProcessEvent(StreamEvent<KqlDynamicRecord> evt)
        {
            if (isDisposed)
            {
                throw new ObjectDisposedException(nameof(KqlEventProcessor));
            }

            if (evt.Payload != null)
                evt.Payload.EventCounter = ++eventCounter;

            input.OnNext(evt);
            lastProcessedSequenceNumber++;

            if (checkpointStopWatch.Elapsed > CheckpointInterval)
            {
                TakeCheckpoint();
            }
        }

        /// <summary>
        /// Flush any pending output
        /// </summary>
        public void Flush()
        {
            if (isDisposed)
            {
                throw new ObjectDisposedException(nameof(KqlEventProcessor));
            }

            queryProcess?.Flush();
        }

        /// <summary>
        /// Create query based on configuration
        /// </summary>
        private void CreateQuery()
        {
            queryContainer = new QueryContainer(new KqlSurrogate());
            input = new Subject<StreamEvent<KqlDynamicRecord>>();

            var inputStream = queryContainer.RegisterInput(
                input,
                DisorderPolicy.Drop(),
                FlushPolicy.FlushOnPunctuation,
                PeriodicPunctuationPolicy.Time(1));

            var windowSize = queryConfig.WindowSize.Ticks;
            var slideSize = queryConfig.SlideSize.Ticks;

            IStreamable<Empty, KqlAggregationResult> query;

            switch (queryConfig.QueryType)
            {
                case KqlQueryType.CountOnly:
                    query = CreateCountQuery(inputStream, windowSize, slideSize);
                    break;

                case KqlQueryType.FieldAggregation:
                    query = CreateFieldAggregationQuery(inputStream, windowSize, slideSize);
                    break;

                case KqlQueryType.MultiMetric:
                    query = CreateMultiMetricQuery(inputStream, windowSize, slideSize);
                    break;

                default:
                    query = CreateCountQuery(inputStream, windowSize, slideSize);
                    break;
            }

            var async = queryContainer.RegisterOutput(query);
            async.Subscribe(new KqlQueryObserver(partitionId, schema, queryConfig));
        }

        /// <summary>
        /// Create a simple count query
        /// </summary>
        private IStreamable<Empty, KqlAggregationResult> CreateCountQuery(
            IStreamable<Empty, KqlDynamicRecord> inputStream,
            long windowSize,
            long slideSize)
        {
            return inputStream
                .HoppingWindowLifetime(windowSize, slideSize)
                .Aggregate(
                    w => w.Count(),
                    w => w.Max(e => e.EventCounter),
                    (count, maxCounter) => new KqlAggregationResult
                    {
                        Count = count,
                        EventCounter = maxCounter,
                        QueryType = "Count"
                    });
        }

        /// <summary>
        /// Create a field aggregation query
        /// </summary>
        private IStreamable<Empty, KqlAggregationResult> CreateFieldAggregationQuery(
            IStreamable<Empty, KqlDynamicRecord> inputStream,
            long windowSize,
            long slideSize)
        {
            var fieldName = queryConfig.AggregationField;
            var column = schema.GetColumn(fieldName);

            return inputStream
                .HoppingWindowLifetime(windowSize, slideSize)
                .Aggregate(
                    w => w.Count(),
                    w => w.Sum(e => e.GetValue(fieldName, 0L)),
                    w => w.Average(e => e.GetValue(fieldName, 0.0)),
                    w => w.Max(e => e.EventCounter),
                    (count, sum, avg, maxCounter) => new KqlAggregationResult
                    {
                        Count = count,
                        Sum = sum,
                        Average = avg,
                        EventCounter = maxCounter,
                        QueryType = $"FieldAggregation({fieldName})",
                        AggregatedField = fieldName
                    });
        }

        /// <summary>
        /// Create a multi-metric query
        /// </summary>
        private IStreamable<Empty, KqlAggregationResult> CreateMultiMetricQuery(
            IStreamable<Empty, KqlDynamicRecord> inputStream,
            long windowSize,
            long slideSize)
        {
            var fieldName = queryConfig.AggregationField;

            return inputStream
                .HoppingWindowLifetime(windowSize, slideSize)
                .Aggregate(
                    w => w.Count(),
                    w => w.Sum(e => e.GetValue(fieldName, 0L)),
                    w => w.Average(e => e.GetValue(fieldName, 0.0)),
                    w => w.Min(e => e.GetValue(fieldName, long.MaxValue)),
                    w => w.Max(e => e.GetValue(fieldName, long.MinValue)),
                    w => w.Max(e => e.EventCounter),
                    (count, sum, avg, min, max, maxCounter) => new KqlAggregationResult
                    {
                        Count = count,
                        Sum = sum,
                        Average = avg,
                        Min = min == long.MaxValue ? 0L : min,
                        Max = max == long.MinValue ? 0L : max,
                        EventCounter = maxCounter,
                        QueryType = $"MultiMetric({fieldName})",
                        AggregatedField = fieldName
                    });
        }

        /// <summary>
        /// Observer for KQL query output
        /// </summary>
        private sealed class KqlQueryObserver : IObserver<StreamEvent<KqlAggregationResult>>
        {
            private readonly string partitionId;
            private readonly KqlTableSchema schema;
            private readonly KqlQueryConfig config;
            private ulong outputCount = 0;
            private ulong totalEventsProcessed = 0;
            private long cumulativeSum = 0;
            private long historicalMin = long.MaxValue;
            private long historicalMax = long.MinValue;

            public KqlQueryObserver(string partitionId, KqlTableSchema schema, KqlQueryConfig config)
            {
                this.partitionId = partitionId;
                this.schema = schema;
                this.config = config;
            }

            public void OnCompleted()
            {
            }

            public void OnError(Exception error)
            {
                Console.WriteLine($"[{partitionId}] Error: {error.Message}");
            }

            public void OnNext(StreamEvent<KqlAggregationResult> value)
            {
                if (value.IsStart)
                {
                    outputCount++;
                    var result = value.Payload;

                    totalEventsProcessed += result.Count;
                    cumulativeSum += result.Sum;

                    if (result.Min < historicalMin && result.Count > 0)
                        historicalMin = result.Min;

                    if (result.Max > historicalMax && result.Count > 0)
                        historicalMax = result.Max;

                    double cumulativeAverage = totalEventsProcessed > 0
                        ? (double)cumulativeSum / totalEventsProcessed
                        : 0.0;

                    var windowDurationTicks = value.EndTime - value.StartTime;
                    double windowDurationSeconds = windowDurationTicks / (double)TimeSpan.TicksPerSecond;
                    double eventsPerSecond = windowDurationSeconds > 0
                        ? result.Count / windowDurationSeconds
                        : 0.0;

                    var startTime = new DateTime(value.StartTime);
                    var endTime = new DateTime(value.EndTime);

                    Console.WriteLine($"\n╔════════════════════════════════════════════════════════════════════╗");
                    Console.WriteLine($"║ [{partitionId}] KQL Window #{outputCount}");
                    Console.WriteLine($"║ Schema: {schema.TableName}");
                    Console.WriteLine($"║ Time: {startTime:HH:mm:ss.fff} - {endTime:HH:mm:ss.fff}");
                    Console.WriteLine($"╠════════════════════════════════════════════════════════════════════╣");
                    Console.WriteLine($"║ Query Type        : {result.QueryType}");
                    Console.WriteLine($"║ Count             : {result.Count, 15:N0}");
                    Console.WriteLine($"║ Event Counter     : {result.EventCounter, 15:N0}");

                    if (!string.IsNullOrEmpty(result.AggregatedField))
                    {
                        Console.WriteLine($"║ Aggregated Field  : {result.AggregatedField}");
                        Console.WriteLine($"║ Sum               : {result.Sum, 15:N0}");
                        Console.WriteLine($"║ Average           : {result.Average, 15:N2}");

                        if (config.QueryType == KqlQueryType.MultiMetric)
                        {
                            Console.WriteLine($"║ Minimum           : {result.Min, 15:N0}");
                            Console.WriteLine($"║ Maximum           : {result.Max, 15:N0}");
                            Console.WriteLine($"║ Range             : {result.Max - result.Min, 15:N0}");
                        }
                    }

                    Console.WriteLine($"║");
                    Console.WriteLine($"║ === Historical Statistics ===");
                    Console.WriteLine($"║ Total Windows     : {outputCount, 15:N0}");
                    Console.WriteLine($"║ Total Events      : {totalEventsProcessed, 15:N0}");
                    Console.WriteLine($"║ Cumulative Sum    : {cumulativeSum, 15:N0}");
                    Console.WriteLine($"║ Cumulative Avg    : {cumulativeAverage, 15:N2}");
                    Console.WriteLine($"║ Events/Second     : {eventsPerSecond, 15:N2}");
                    Console.WriteLine($"╚════════════════════════════════════════════════════════════════════╝");
                }
            }
        }

        /// <summary>
        /// Take a checkpoint of the current query state
        /// </summary>
        private void TakeCheckpoint()
        {
            Console.WriteLine($"Taking checkpoint at sequence {lastProcessedSequenceNumber}, event counter: {eventCounter}");

            var checkpointFileName = $"{partitionId}-{lastProcessedSequenceNumber}.checkpoint";
            var checkpointPath = Path.Combine(checkpointDirectory, checkpointFileName);
            var tempPath = checkpointPath + ".tmp";
            var metadataPath = Path.Combine(checkpointDirectory, $"{partitionId}-{lastProcessedSequenceNumber}.metadata");

            try
            {
                using (var stream = File.Create(tempPath))
                {
                    queryProcess.Checkpoint(stream);
                    stream.Flush();
                }

                var metadataContent = $"{eventCounter}:{lastProcessedSequenceNumber}";
                File.WriteAllText(metadataPath, metadataContent);

                if (File.Exists(checkpointPath))
                {
                    File.Delete(checkpointPath);
                }

                File.Move(tempPath, checkpointPath);
                DeleteOlderCheckpoints(checkpointFileName);
                checkpointStopWatch.Restart();
                Console.WriteLine($"Checkpoint saved successfully");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error taking checkpoint: {ex.Message}");
                if (File.Exists(tempPath))
                    try { File.Delete(tempPath); }
                    catch { }
                if (File.Exists(metadataPath))
                    try { File.Delete(metadataPath); }
                    catch { }
            }
        }

        private string GetCheckpointWithHighestCounter()
        {
            if (!Directory.Exists(checkpointDirectory))
                return null;

            var checkpointFiles = Directory.GetFiles(checkpointDirectory, $"{partitionId}-*.checkpoint");
            if (checkpointFiles.Length == 0)
                return null;

            string bestCheckpoint = null;
            long highestCounter = -1;

            foreach (var checkpointFile in checkpointFiles)
            {
                var fileName = Path.GetFileNameWithoutExtension(checkpointFile);
                var parts = fileName.Split('-');
                if (parts.Length > 2 && long.TryParse(parts[2], out long seqNum))
                {
                    var metadataPath = Path.Combine(checkpointDirectory, $"{partitionId}-{seqNum}.metadata");
                    if (File.Exists(metadataPath))
                    {
                        try
                        {
                            var metadataText = File.ReadAllText(metadataPath);
                            long counter = 0;

                            if (metadataText.Contains(':'))
                            {
                                var metadataParts = metadataText.Split(':');
                                if (metadataParts.Length == 2 && long.TryParse(metadataParts[0], out counter))
                                {
                                }
                            }
                            else
                            {
                                long.TryParse(metadataText, out counter);
                            }

                            if (counter > highestCounter)
                            {
                                highestCounter = counter;
                                bestCheckpoint = checkpointFile;
                            }
                        }
                        catch
                        {
                            continue;
                        }
                    }
                }
            }

            return bestCheckpoint;
        }

        private void DeleteOlderCheckpoints(string currentCheckpointFileName)
        {
            try
            {
                var currentSeqNum = ExtractSequenceNumber(Path.GetFileNameWithoutExtension(currentCheckpointFileName));
                if (!currentSeqNum.HasValue)
                    return;

                var currentMetadataPath = Path.Combine(checkpointDirectory, $"{partitionId}-{currentSeqNum}.metadata");
                if (!File.Exists(currentMetadataPath))
                    return;

                long currentCounter = eventCounter;
                var checkpointFiles = Directory.GetFiles(checkpointDirectory, $"{partitionId}-*.checkpoint");

                foreach (var file in checkpointFiles)
                {
                    var fileName = Path.GetFileName(file);
                    if (fileName == currentCheckpointFileName)
                        continue;

                    var seqNum = ExtractSequenceNumber(Path.GetFileNameWithoutExtension(file));
                    if (!seqNum.HasValue)
                        continue;

                    var metadataFile = Path.Combine(checkpointDirectory, $"{partitionId}-{seqNum}.metadata");
                    if (File.Exists(metadataFile))
                    {
                        try
                        {
                            var metadataText = File.ReadAllText(metadataFile);
                            long fileCounter = 0;

                            if (metadataText.Contains(':'))
                            {
                                var parts = metadataText.Split(':');
                                if (parts.Length > 0)
                                    long.TryParse(parts[0], out fileCounter);
                            }
                            else
                            {
                                long.TryParse(metadataText, out fileCounter);
                            }

                            if (fileCounter < currentCounter)
                            {
                                File.Delete(file);
                                File.Delete(metadataFile);
                                Console.WriteLine($"Deleted old checkpoint: {fileName}");
                            }
                        }
                        catch
                        {
                        }
                    }
                }
            }
            catch
            {
            }
        }

        private long? ExtractSequenceNumber(string fileName)
        {
            var parts = fileName.Split('-');
            if (parts.Length > 1 && long.TryParse(parts[1], out long seqNum))
                return seqNum;
            return null;
        }

        public void Dispose()
        {
            if (isDisposed)
                return;

            if (queryProcess != null)
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

            input?.OnCompleted();
            input?.Dispose();
            checkpointStopWatch?.Stop();

            isDisposed = true;
            Console.WriteLine($"KqlEventProcessor disposed");
        }
    }

    /// <summary>
    /// Result structure for KQL aggregations
    /// </summary>
    public struct KqlAggregationResult
    {
        public ulong Count { get; set; }
        public long Sum { get; set; }
        public double Average { get; set; }
        public long Min { get; set; }
        public long Max { get; set; }
        public long EventCounter { get; set; }
        public string QueryType { get; set; }
        public string AggregatedField { get; set; }
    }

    /// <summary>
    /// Configuration for KQL queries
    /// </summary>
    public sealed class KqlQueryConfig
    {
        public KqlQueryType QueryType { get; set; }
        public TimeSpan WindowSize { get; set; }
        public TimeSpan SlideSize { get; set; }
        public string AggregationField { get; set; }

        public static KqlQueryConfig CreateDefault()
        {
            return new KqlQueryConfig
            {
                QueryType = KqlQueryType.CountOnly,
                WindowSize = TimeSpan.FromSeconds(5),
                SlideSize = TimeSpan.FromSeconds(1),
                AggregationField = null
            };
        }

        public static KqlQueryConfig CreateFieldAggregation(string fieldName, TimeSpan? windowSize = null, TimeSpan? slideSize = null)
        {
            return new KqlQueryConfig
            {
                QueryType = KqlQueryType.FieldAggregation,
                WindowSize = windowSize ?? TimeSpan.FromSeconds(5),
                SlideSize = slideSize ?? TimeSpan.FromSeconds(1),
                AggregationField = fieldName
            };
        }

        public static KqlQueryConfig CreateMultiMetric(string fieldName, TimeSpan? windowSize = null, TimeSpan? slideSize = null)
        {
            return new KqlQueryConfig
            {
                QueryType = KqlQueryType.MultiMetric,
                WindowSize = windowSize ?? TimeSpan.FromSeconds(5),
                SlideSize = slideSize ?? TimeSpan.FromSeconds(1),
                AggregationField = fieldName
            };
        }
    }

    /// <summary>
    /// Types of KQL queries
    /// </summary>
    public enum KqlQueryType
    {
        CountOnly,
        FieldAggregation,
        MultiMetric
    }
}
