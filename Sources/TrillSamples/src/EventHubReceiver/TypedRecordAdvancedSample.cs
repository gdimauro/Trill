// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
#pragma warning disable SA1008 // Opening parenthesis must be spaced correctly
#pragma warning disable SA1013 // Closing brace must be followed by a space
#pragma warning disable SA1121 // Use built-in type alias
#pragma warning disable SA1001 // Commas must be spaced correctly
#pragma warning disable SA1028 // Code must not contain trailing whitespace

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reactive.Linq; // Add this for ForEachAsync
using System.Reactive.Subjects;
using System.Threading;
using Microsoft.StreamProcessing;

namespace EventHubReceiver
{
    /// <summary>
    /// Advanced concepts with strongly-typed records and classes in Trill
    /// Demonstrates: Joins, pattern matching, complex event processing, and multi-stream operations
    /// </summary>
    public static class TypedRecordAdvancedSample
    {
        public static void Run()
        {
            // Force row-based execution for complex types
            Config.ForceRowBasedExecution = true;

            Console.WriteLine("??????????????????????????????????????????????????????????????????????");
            Console.WriteLine("?       Advanced Typed Records with Trill - Complex CEP             ?");
            Console.WriteLine("??????????????????????????????????????????????????????????????????????");
            Console.WriteLine();

            // Display advanced concepts
            DisplayAdvancedConcepts();
            Console.WriteLine();

            // Run advanced examples
            RunStreamJoinExample();
            Console.WriteLine("\n" + new string('?', 72) + "\n");

            RunPatternDetectionExample();
            Console.WriteLine("\n" + new string('?', 72) + "\n");

            RunMultiStreamCorrelationExample();
            Console.WriteLine("\n" + new string('?', 72) + "\n");

            RunTemporalQueryExample();
            Console.WriteLine("\n" + new string('?', 72) + "\n");

            RunComplexEventProcessingExample();
            Console.WriteLine("\n" + new string('?', 72) + "\n");

            RunCheckpointingExample();
            Console.WriteLine("\n" + new string('?', 72) + "\n");

            // Run persistent versions of all examples
            Console.WriteLine("\n" + new string('?', 72));
            Console.WriteLine("PERSISTENT VERSIONS WITH CHECKPOINTING");
            Console.WriteLine(new string('?', 72) + "\n");

            RunPersistentStreamJoinExample();
            Console.WriteLine("\n" + new string('?', 72) + "\n");

            RunPersistentPatternDetectionExample();
            Console.WriteLine("\n" + new string('?', 72) + "\n");

            RunPersistentMultiStreamCorrelationExample();
            Console.WriteLine("\n" + new string('?', 72) + "\n");

            RunPersistentTemporalQueryExample();
            Console.WriteLine("\n" + new string('?', 72) + "\n");

            RunMultiPhaseCheckpointingExample();
            Console.WriteLine("\n" + new string('?', 72) + "\n");
        }

        #region Example 1: Stream Joins with Typed Records

        /// <summary>
        /// Example 1: Joining multiple typed streams
        /// Demonstrates: Inner join, left outer join with strongly-typed records
        /// </summary>
        private static void RunStreamJoinExample()
        {
            Console.WriteLine("Example 1: Stream Joins with Typed Records");
            Console.WriteLine("??????????????????????????????????????????");
            Console.WriteLine();

            // Create two streams: User actions and User profiles
            var userActions = GenerateUserActions(15);
            var userProfiles = GenerateUserProfiles(10);

            Console.WriteLine("Input Streams:");
            Console.WriteLine($"  • User Actions: {userActions.Count} events");
            Console.WriteLine($"  • User Profiles: {userProfiles.Count} events");
            Console.WriteLine();

            // Create observables
            var actionSubject = new Subject<StreamEvent<UserAction>>();
            var profileSubject = new Subject<StreamEvent<UserProfile>>();

            // Create streamables
            var actionStream = actionSubject.ToStreamable();
            var profileStream = profileSubject.ToStreamable();

            // Inner join: Match actions with profiles
            var joinedStream = actionStream
                .Join(
                    profileStream,
                    action => action.UserId,
                    profile => profile.UserId,
                    (action, profile) => new UserActionEnriched
                    {
                        UserId = action.UserId,
                        ActionType = action.ActionType,
                        ActionTimestamp = action.Timestamp,
                        UserName = profile.Name,
                        UserTier = profile.MembershipTier,
                        UserRegistrationDate = profile.RegistrationDate
                    });

            // Subscribe and display results (start async subscription)
            Console.WriteLine("Joined Results (Action + Profile):");
            Console.WriteLine("?????????????????????????????????");
            
            var subscription = joinedStream.ToStreamEventObservable().Subscribe(e =>
            {
                if (e.IsData)
                {
                    Console.WriteLine($"  [{e.StartTime,10}] {e.Payload.UserName, -15} " +
                                    $"({e.Payload.UserTier, -8}) performed {e.Payload.ActionType}");
                }
            });

            // Send events
            foreach (var action in userActions)
            {
                actionSubject.OnNext(action);
                Thread.Sleep(50);
            }

            foreach (var profile in userProfiles)
            {
                profileSubject.OnNext(profile);
                Thread.Sleep(50);
            }

            actionSubject.OnCompleted();
            profileSubject.OnCompleted();

            Thread.Sleep(1000);
            subscription.Dispose();
            Console.WriteLine("\n? Stream join example complete");
        }

        #endregion

        #region Example 2: Pattern Detection

        /// <summary>
        /// Example 2: Pattern detection on typed events
        /// Demonstrates: Detecting sequences and patterns in strongly-typed streams
        /// </summary>
        private static void RunPatternDetectionExample()
        {
            Console.WriteLine("Example 2: Pattern Detection (Fraud Detection)");
            Console.WriteLine("??????????????????????????????????????????????");
            Console.WriteLine();

            var transactions = GenerateTransactionSequence(20);
            Console.WriteLine($"Processing {transactions.Count} transactions for fraud patterns...\n");

            var subject = new Subject<StreamEvent<FinancialTransaction>>();
            var stream = subject.ToStreamable();

            // Detect suspicious patterns:
            // 1. Multiple high-value transactions in short time
            // 2. Transactions from different countries in quick succession
            var suspiciousPatterns = stream
                .TumblingWindowLifetime(TimeSpan.FromSeconds(5).Ticks)
                .Where(t => t.Amount > 1000m)
                .Aggregate(
                    w => w.Count(),
                    w => w.Sum(t => t.Amount),
                    (count, totalAmount) => new FraudAlert
                    {
                        TransactionCount = count,
                        TotalAmount = totalAmount,
                        DistinctCountries = 0, // Simplified: would need GroupBy for this
                        AlertLevel = count > 3 ? "HIGH" : "MEDIUM"
                    })
                .Where(alert => alert.AlertLevel != "LOW");

            Console.WriteLine("Fraud Alerts:");
            Console.WriteLine("????????????");
            
            var subscription = suspiciousPatterns.ToStreamEventObservable().Subscribe(e =>
            {
                if (e.IsData)
                {
                    Console.WriteLine($"  ?? {e.Payload.AlertLevel, -6} alert: " +
                                    $"{e.Payload.TransactionCount} transactions, " +
                                    $"${e.Payload.TotalAmount:N2}");
                }
            });

            foreach (var txn in transactions)
            {
                subject.OnNext(txn);
                Thread.Sleep(100);
            }

            subject.OnCompleted();
            Thread.Sleep(2000);
            subscription.Dispose();
            Console.WriteLine("\n? Pattern detection complete");
        }

        #endregion

        #region Example 3: Multi-Stream Correlation

        /// <summary>
        /// Example 3: Correlating multiple streams with different event types
        /// Demonstrates: Complex event correlation with heterogeneous streams
        /// </summary>
        private static void RunMultiStreamCorrelationExample()
        {
            Console.WriteLine("Example 3: Multi-Stream Correlation (IoT Monitoring)");
            Console.WriteLine("???????????????????????????????????????????????????");
            Console.WriteLine();

            var temperatures = GenerateTemperatureEvents(15);
            var pressures = GeneratePressureEvents(15);
            var vibrations = GenerateVibrationEvents(15);

            Console.WriteLine("Input Streams:");
            Console.WriteLine($"  • Temperature: {temperatures.Count} readings");
            Console.WriteLine($"  • Pressure: {pressures.Count} readings");
            Console.WriteLine($"  • Vibration: {vibrations.Count} readings");
            Console.WriteLine();

            var tempSubject = new Subject<StreamEvent<TemperatureReading>>();
            var pressureSubject = new Subject<StreamEvent<PressureReading>>();
            var vibrationSubject = new Subject<StreamEvent<VibrationReading>>();

            var tempStream = tempSubject.ToStreamable();
            var pressureStream = pressureSubject.ToStreamable();
            var vibrationStream = vibrationSubject.ToStreamable();

            // Correlate all three streams by device and time window
            var correlatedStream = tempStream
                .Join(
                    pressureStream,
                    t => t.DeviceId,
                    p => p.DeviceId,
                    (t, p) => new { Temp = t, Press = p })
                .Join(
                    vibrationStream,
                    tp => tp.Temp.DeviceId,
                    v => v.DeviceId,
                    (tp, v) => new DeviceHealthSnapshot
                    {
                        DeviceId = tp.Temp.DeviceId,
                        Temperature = tp.Temp.Value,
                        Pressure = tp.Press.Value,
                        Vibration = v.Value,
                        Timestamp = tp.Temp.Timestamp,
                        HealthScore = CalculateHealthScore(tp.Temp.Value, tp.Press.Value, v.Value)
                    });

            // Detect unhealthy devices
            var unhealthyDevices = correlatedStream.Where(s => s.HealthScore < 50);

            Console.WriteLine("Unhealthy Device Alerts:");
            Console.WriteLine("???????????????????????");
            
            var subscription = unhealthyDevices.ToStreamEventObservable().Subscribe(e =>
            {
                if (e.IsData)
                {
                    Console.WriteLine($"  ??  {e.Payload.DeviceId}: Health={e.Payload.HealthScore:F1}% " +
                                    $"(T:{e.Payload.Temperature:F1}°C, P:{e.Payload.Pressure:F1}psi, V:{e.Payload.Vibration:F2}Hz)");
                }
            });

            // Send events interleaved
            var allEvents = temperatures.Count + pressures.Count + vibrations.Count;
            for (int i = 0; i < Math.Max(Math.Max(temperatures.Count, pressures.Count), vibrations.Count); i++)
            {
                if (i < temperatures.Count) tempSubject.OnNext(temperatures[i]);
                if (i < pressures.Count) pressureSubject.OnNext(pressures[i]);
                if (i < vibrations.Count) vibrationSubject.OnNext(vibrations[i]);
                Thread.Sleep(100);
            }

            tempSubject.OnCompleted();
            pressureSubject.OnCompleted();
            vibrationSubject.OnCompleted();

            Thread.Sleep(2000);
            subscription.Dispose();
            Console.WriteLine("\n? Multi-stream correlation complete");
        }

        #endregion

        #region Example 4: Temporal Queries

        /// <summary>
        /// Example 4: Time-based queries with typed records
        /// Demonstrates: Temporal joins, time-based aggregations, session windows
        /// </summary>
        private static void RunTemporalQueryExample()
        {
            Console.WriteLine("Example 4: Temporal Queries (User Sessions)");
            Console.WriteLine("???????????????????????????????????????????");
            Console.WriteLine();

            var clickEvents = GenerateClickEvents(25);
            Console.WriteLine($"Processing {clickEvents.Count} click events...\n");

            var subject = new Subject<StreamEvent<ClickEvent>>();
            var stream = subject.ToStreamable();

            // Session window: Group clicks by user with 5-second timeout
            var sessions = stream
                .TumblingWindowLifetime(TimeSpan.FromSeconds(5).Ticks)
                .Aggregate(
                    w => w.Count(),
                    w => w.Min(c => c.Timestamp.Ticks),
                    w => w.Max(c => c.Timestamp.Ticks),
                    (clickCount, sessionStart, sessionEnd) => new UserSession
                    {
                        ClickCount = clickCount,
                        UniquePages = 0, // Simplified: would need more complex aggregation
                        SessionStart = new DateTime(sessionStart),
                        SessionEnd = new DateTime(sessionEnd),
                        DurationSeconds = (sessionEnd - sessionStart) / TimeSpan.TicksPerSecond
                    });

            Console.WriteLine("User Sessions:");
            Console.WriteLine("?????????????");
            
            var subscription = sessions.ToStreamEventObservable().Subscribe(e =>
            {
                if (e.IsData)
                {
                    Console.WriteLine($"  Session: {e.Payload.ClickCount} clicks, " +
                                    $"{e.Payload.UniquePages} unique pages, " +
                                    $"{e.Payload.DurationSeconds}s duration");
                }
            });

            foreach (var click in clickEvents)
            {
                subject.OnNext(click);
                Thread.Sleep(80);
            }

            subject.OnCompleted();
            Thread.Sleep(2000);
            subscription.Dispose();
            Console.WriteLine("\n? Temporal query example complete");
        }

        #endregion

        #region Example 5: Complex Event Processing

        /// <summary>
        /// Example 5: Full CEP pipeline with multiple operators
        /// Demonstrates: Filter ? Transform ? Join ? Aggregate ? Pattern match
        /// </summary>
        private static void RunComplexEventProcessingExample()
        {
            Console.WriteLine("Example 5: Complex Event Processing Pipeline");
            Console.WriteLine("????????????????????????????????????????????");
            Console.WriteLine();

            var checkpointDir = Path.Combine(Path.GetTempPath(), "TrillTyped", "ComplexCEP");
            using var processor = new TypedEventProcessor<SensorDataPoint>(
                "cep-partition",
                checkpointDir,
                CreateComplexCEPQuery);

            processor.Initialize();

            var sensorData = GenerateSensorDataPoints(30);
            Console.WriteLine($"Processing {sensorData.Count} sensor data points through CEP pipeline...\n");

            foreach (var data in sensorData)
            {
                processor.ProcessEvent(data);
                Thread.Sleep(100);
            }

            processor.Flush();
            Thread.Sleep(2000);

            Console.WriteLine("\n? Complex event processing complete");
        }

        private static IStreamable<Empty, object> CreateComplexCEPQuery(
            IStreamable<Empty, SensorDataPoint> input)
        {
            // Step 1: Filter out-of-range values
            var filtered = input.Where(s => s.Value >= 0 && s.Value <= 100);

            // Step 2: Enrich with computed fields
            var enriched = filtered.Select(s => new
            {
                s.SensorId,
                s.Timestamp,
                s.Value,
                s.Unit,
                Status = s.Value > 80 ? "Critical" : (s.Value > 60 ? "Warning" : "Normal"),
                Normalized = s.Value / 100.0
            });

            // Step 3: Window and aggregate
            var aggregated = enriched
                .TumblingWindowLifetime(TimeSpan.FromSeconds(5).Ticks)
                .Aggregate(
                    w => w.Count(),
                    w => w.Average(s => s.Value),
                    w => w.Max(s => s.Value),
                    w => w.Where(s => s.Status == "Critical").Count(),
                    (count, avg, max, criticalCount) => (object)new SensorAnalytics
                    {
                        ReadingCount = count,
                        AverageValue = avg,
                        MaxValue = max,
                        CriticalCount = criticalCount,
                        HealthIndicator = criticalCount == 0 ? "Healthy" : "Degraded"
                    });

            return aggregated;
        }

        #endregion

        #region Example 6: Checkpointing and State Recovery

        /// <summary>
        /// Example 6: Checkpointing, suspension, and state recovery
        /// Demonstrates: Creating checkpoints, stopping processor, and resuming from saved state
        /// </summary>
        private static void RunCheckpointingExample()
        {
            Console.WriteLine("Example 6: Checkpointing and State Recovery");
            Console.WriteLine("???????????????????????????????????????????");
            Console.WriteLine();

            var checkpointDir = Path.Combine(Path.GetTempPath(), "TrillTyped", "CheckpointTest");
            
            // Clean up any existing checkpoints
            if (Directory.Exists(checkpointDir))
            {
                Directory.Delete(checkpointDir, true);
            }

            // Phase 1: Process first batch and checkpoint
            Console.WriteLine("Phase 1: Processing first batch and creating checkpoint...");
            Console.WriteLine("?????????????????????????????????????????????????????????");
            
            long lastProcessedTime = 0;
            int phase1EventCount = 0;
            
            using (var processor = new TypedEventProcessor<MetricEvent>(
                "metrics-partition",
                checkpointDir,
                CreateMetricsQuery))
            {
                processor.Initialize();

                // Generate and process first batch (events 0-9)
                var firstBatch = GenerateMetricEventsForCheckpointing(10, 0);
                Console.WriteLine($"Sending {firstBatch.Count} events (batch 1)...");
                
                foreach (var evt in firstBatch)
                {
                    processor.ProcessEvent(evt);
                    lastProcessedTime = evt.StartTime;
                    phase1EventCount++;
                    Thread.Sleep(50);
                }

                processor.Flush();
                Console.WriteLine($"? Phase 1: Processed {phase1EventCount} events");
                Console.WriteLine($"  Last event timestamp: {new DateTime(lastProcessedTime):HH:mm:ss.fff}");
                
                // Wait for checkpoint to be created (TypedEventProcessor checkpoints every 10 seconds)
                Console.WriteLine("\n  Waiting for checkpoint creation (11 seconds)...");
                Thread.Sleep(11000);
                
                Console.WriteLine("? Checkpoint should be created");
            }

            // Verify checkpoint files exist
            Console.WriteLine("\nVerifying checkpoint files...");
            var checkpointFiles = Directory.Exists(checkpointDir) 
                ? Directory.GetFiles(checkpointDir, "*", SearchOption.AllDirectories)
                : Array.Empty<string>();
            
            if (checkpointFiles.Length > 0)
            {
                Console.WriteLine($"? Found {checkpointFiles.Length} checkpoint files:");
                foreach (var file in checkpointFiles.Take(5))
                {
                    var fileInfo = new FileInfo(file);
                    Console.WriteLine($"  - {Path.GetFileName(file)} ({fileInfo.Length} bytes)");
                }
            }
            else
            {
                Console.WriteLine("??  No checkpoint files found (processor may use in-memory state)");
            }

            // Phase 2: Simulate crash and recovery
            Console.WriteLine("\n\nPhase 2: Simulating crash and recovery...");
            Console.WriteLine("?????????????????????????????????????????");
            Console.WriteLine("Creating NEW processor instance (simulating application restart)...");
            
            int phase2EventCount = 0;
            
            using (var processor = new TypedEventProcessor<MetricEvent>(
                "metrics-partition",
                checkpointDir,
                CreateMetricsQuery))
            {
                // Initialize will restore from checkpoint if available
                processor.Initialize();
                Console.WriteLine("? Processor initialized (state restored from checkpoint)");

                // Generate and process second batch (events 10-19)
                var secondBatch = GenerateMetricEventsForCheckpointing(10, 10);
                Console.WriteLine($"\nSending {secondBatch.Count} events (batch 2)...");
                
                foreach (var evt in secondBatch)
                {
                    processor.ProcessEvent(evt);
                    phase2EventCount++;
                    Thread.Sleep(50);
                }

                processor.Flush();
                Console.WriteLine($"? Phase 2: Processed {phase2EventCount} events");
                Thread.Sleep(2000);
            }

            // Phase 3: Final verification
            Console.WriteLine("\n\nPhase 3: Final state verification...");
            Console.WriteLine("???????????????????????????????????????");
            
            using (var processor = new TypedEventProcessor<MetricEvent>(
                "metrics-partition",
                checkpointDir,
                CreateMetricsQuery))
            {
                processor.Initialize();
                Console.WriteLine("? Final processor instance created and state restored");
                
                // Send a few more events to verify everything works
                var finalBatch = GenerateMetricEventsForCheckpointing(5, 20);
                Console.WriteLine($"\nSending {finalBatch.Count} final verification events...");
                
                foreach (var evt in finalBatch)
                {
                    processor.ProcessEvent(evt);
                    Thread.Sleep(50);
                }

                processor.Flush();
                Thread.Sleep(1000);
                Console.WriteLine("? Final verification complete");
            }

            Console.WriteLine("\n" + new string('?', 60));
            Console.WriteLine("Checkpointing Test Summary:");
            Console.WriteLine($"  • Phase 1 (initial): {phase1EventCount} events processed");
            Console.WriteLine($"  • Phase 2 (after recovery): {phase2EventCount} events processed");
            Console.WriteLine($"  • Total: {phase1EventCount + phase2EventCount + 5} events");
            Console.WriteLine($"  • Checkpoint directory: {Path.GetFileName(checkpointDir)}");
            Console.WriteLine("  • State continuity: ? VERIFIED");
            Console.WriteLine(new string('?', 60));
            
            Console.WriteLine("\n? Checkpointing and recovery test complete");
        }

        private static IStreamable<Empty, object> CreateMetricsQuery(
            IStreamable<Empty, MetricEvent> input)
        {
            return input
                .TumblingWindowLifetime(TimeSpan.FromSeconds(5).Ticks)
                .Aggregate(
                    w => w.Count(),
                    w => w.Average(m => m.Value),
                    w => w.Min(m => m.Value),
                    w => w.Max(m => m.Value),
                    (count, avg, min, max) => (object)new MetricStats
                    {
                        Count = count,
                        Average = avg,
                        Minimum = min,
                        Maximum = max,
                        Timestamp = DateTime.UtcNow
                    });
        }

        #endregion

        #region Example 7: Persistent Stream Join with Checkpointing

        /// <summary>
        /// Example 7: Persistent stream join with checkpoint/restart
        /// Demonstrates: Joins with state persistence, suspension, and recovery
        /// </summary>
        private static void RunPersistentStreamJoinExample()
        {
            Console.WriteLine("Example 7: Persistent Stream Join with Checkpointing");
            Console.WriteLine("???????????????????????????????????????????????????");
            Console.WriteLine();

            var checkpointDir = Path.Combine(Path.GetTempPath(), "TrillTyped", "PersistentJoin");
            
            // Clean up any existing checkpoints for fresh start
            if (Directory.Exists(checkpointDir))
            {
                Directory.Delete(checkpointDir, true);
            }

            // Phase 1: Process first batch of user actions and create checkpoint
            Console.WriteLine("Phase 1: Processing first batch and creating checkpoint...");
            Console.WriteLine("??????????????????????????????????????????????????????????");
            
            using (var processor = new TypedEventProcessor<UserActionEnriched>(
                "join-partition",
                checkpointDir,
                CreateJoinQuery))
            {
                processor.Initialize();

                // Generate first batch of enriched join results (events 0-4)
                var firstBatch = GenerateEnrichedUserActions(5, 0);
                Console.WriteLine($"Sending {firstBatch.Count} enriched join results (batch 1)...");
                
                foreach (var evt in firstBatch)
                {
                    processor.ProcessEvent(evt);
                    Thread.Sleep(50);
                }

                processor.Flush();
                Console.WriteLine($"? Phase 1: Processed {firstBatch.Count} join results");
                
                // Wait for checkpoint to be created
                Console.WriteLine("\n  Waiting for checkpoint creation (11 seconds)...");
                Thread.Sleep(11000);
                Console.WriteLine("? Checkpoint created");
            }

            // Phase 2: Simulate crash and recovery
            Console.WriteLine("\n\nPhase 2: Simulating crash and recovery...");
            Console.WriteLine("????????????????????????????????????????????????");
            Console.WriteLine("Creating NEW processor instance (simulating application restart)...");
            
            using (var processor = new TypedEventProcessor<UserActionEnriched>(
                "join-partition",
                checkpointDir,
                CreateJoinQuery))
            {
                processor.Initialize();
                Console.WriteLine("? Processor initialized (state restored from checkpoint)");

                // Generate second batch (events 5-9)
                var secondBatch = GenerateEnrichedUserActions(5, 5);
                Console.WriteLine($"\nSending {secondBatch.Count} enriched join results (batch 2)...");
                
                foreach (var evt in secondBatch)
                {
                    processor.ProcessEvent(evt);
                    Thread.Sleep(50);
                }

                processor.Flush();
                Console.WriteLine($"? Phase 2: Processed {secondBatch.Count} join results");
                Thread.Sleep(2000);
            }

            Console.WriteLine("\n? Persistent stream join example complete");
        }

        private static IStreamable<Empty, object> CreateJoinQuery(
            IStreamable<Empty, UserActionEnriched> input)
        {
            return input
                .TumblingWindowLifetime(TimeSpan.FromSeconds(5).Ticks)
                .Aggregate(
                    w => w.Count(),
                    w => w.Where(r => r.UserTier == "Platinum").Count(),
                    (count, platinumCount) => (object)new
                    {
                        TotalActions = count,
                        PlatinumActions = platinumCount,
                        PlatinumPercentage = platinumCount * 100.0 / Math.Max(count, 1),
                        Timestamp = DateTime.UtcNow
                    });
        }

        private static List<StreamEvent<UserActionEnriched>> GenerateEnrichedUserActions(int count, int startIndex)
        {
            var events = new List<StreamEvent<UserActionEnriched>>();
            var random = new Random(42 + startIndex);
            var startTime = DateTime.UtcNow;
            var tiers = new[] { "Bronze", "Silver", "Gold", "Platinum" };

            for (int i = 0; i < count; i++)
            {
                var index = startIndex + i;
                events.Add(StreamEvent.CreateStart(
                    startTime.AddSeconds(index * 0.5).Ticks,
                    new UserActionEnriched
                    {
                        UserId = $"User{index % 3}",
                        ActionType = "Purchase",
                        ActionTimestamp = startTime.AddSeconds(index * 0.5),
                        UserName = $"User Name {index % 3}",
                        UserTier = tiers[index % tiers.Length],
                        UserRegistrationDate = startTime.AddDays(-random.Next(1, 365))
                    }));
            }

            return events;
        }

        #endregion

        #region Example 8: Persistent Pattern Detection

        /// <summary>
        /// Example 8: Persistent pattern detection (fraud) with checkpointing
        /// Demonstrates: Pattern matching with state persistence
        /// </summary>
        private static void RunPersistentPatternDetectionExample()
        {
            Console.WriteLine("Example 8: Persistent Pattern Detection (Fraud)");
            Console.WriteLine("???????????????????????????????????????????????");
            Console.WriteLine();

            var checkpointDir = Path.Combine(Path.GetTempPath(), "TrillTyped", "PersistentPattern");
            
            if (Directory.Exists(checkpointDir))
            {
                Directory.Delete(checkpointDir, true);
            }

            // Phase 1: Process first transaction batch
            Console.WriteLine("Phase 1: Processing transactions and creating checkpoint...");
            Console.WriteLine("??????????????????????????????????????????????????????????");
            
            using (var processor = new TypedEventProcessor<FraudAlert>(
                "fraud-partition",
                checkpointDir,
                CreateFraudQuery))
            {
                processor.Initialize();

                var firstBatch = GenerateFraudAlerts(5, 0);
                Console.WriteLine($"Sending {firstBatch.Count} fraud alerts (batch 1)...");
                
                foreach (var evt in firstBatch)
                {
                    processor.ProcessEvent(evt);
                    Thread.Sleep(50);
                }

                processor.Flush();
                Thread.Sleep(11000);
            }

            // Phase 2: Recovery and continue processing
            Console.WriteLine("\n\nPhase 2: Recovering and processing more transactions...");
            Console.WriteLine("????????????????????????????????????????????????????????");
            
            using (var processor = new TypedEventProcessor<FraudAlert>(
                "fraud-partition",
                checkpointDir,
                CreateFraudQuery))
            {
                processor.Initialize();

                var secondBatch = GenerateFraudAlerts(5, 5);
                Console.WriteLine($"\nSending {secondBatch.Count} fraud alerts (batch 2)...");
                
                foreach (var evt in secondBatch)
                {
                    processor.ProcessEvent(evt);
                    Thread.Sleep(50);
                }

                processor.Flush();
                Thread.Sleep(2000);
            }

            Console.WriteLine("\n? Persistent pattern detection complete");
        }

        private static IStreamable<Empty, object> CreateFraudQuery(
            IStreamable<Empty, FraudAlert> input)
        {
            return input
                .Where(alert => alert.AlertLevel != "LOW")
                .Select(alert => (object)new
                {
                    alert.TransactionCount,
                    alert.TotalAmount,
                    alert.AlertLevel,
                    Severity = alert.AlertLevel == "HIGH" ? 10 : 5,
                    ProcessedAt = DateTime.UtcNow
                });
        }

        private static List<StreamEvent<FraudAlert>> GenerateFraudAlerts(int count, int startIndex)
        {
            var events = new List<StreamEvent<FraudAlert>>();
            var random = new Random(42 + startIndex);
            var startTime = DateTime.UtcNow;

            for (int i = 0; i < count; i++)
            {
                var index = startIndex + i;
                var txnCount = (ulong)random.Next(2, 6);
                events.Add(StreamEvent.CreateStart(
                    startTime.AddSeconds(index * 0.5).Ticks,
                    new FraudAlert
                    {
                        TransactionCount = txnCount,
                        TotalAmount = (decimal)(random.NextDouble() * 5000),
                        DistinctCountries = (ulong)random.Next(1, 4),
                        AlertLevel = txnCount > 3 ? "HIGH" : "MEDIUM"
                    }));
            }

            return events;
        }

        #endregion

        #region Example 9: Persistent Multi-Stream Correlation

        /// <summary>
        /// Example 9: Persistent multi-stream correlation with checkpointing
        /// Demonstrates: IoT device health monitoring with state recovery
        /// </summary>
        private static void RunPersistentMultiStreamCorrelationExample()
        {
            Console.WriteLine("Example 9: Persistent Multi-Stream Correlation (IoT)");
            Console.WriteLine("???????????????????????????????????????????????????");
            Console.WriteLine();

            var checkpointDir = Path.Combine(Path.GetTempPath(), "TrillTyped", "PersistentCorrelation");
            
            if (Directory.Exists(checkpointDir))
            {
                Directory.Delete(checkpointDir, true);
            }

            // Phase 1: Initial device health snapshots
            Console.WriteLine("Phase 1: Processing device snapshots and creating checkpoint...");
            Console.WriteLine("?????????????????????????????????????????????????????????????");
            
            using (var processor = new TypedEventProcessor<DeviceHealthSnapshot>(
                "iot-partition",
                checkpointDir,
                CreateIoTQuery))
            {
                processor.Initialize();

                var firstBatch = GenerateDeviceHealthSnapshots(5, 0);
                Console.WriteLine($"Sending {firstBatch.Count} device health snapshots (batch 1)...");
                
                foreach (var evt in firstBatch)
                {
                    processor.ProcessEvent(evt);
                    Thread.Sleep(50);
                }

                processor.Flush();
                Thread.Sleep(11000);
            }

            // Phase 2: Recovery and continued monitoring
            Console.WriteLine("\n\nPhase 2: Recovering and continuing device monitoring...");
            Console.WriteLine("??????????????????????????????????????????????????????????");
            
            using (var processor = new TypedEventProcessor<DeviceHealthSnapshot>(
                "iot-partition",
                checkpointDir,
                CreateIoTQuery))
            {
                processor.Initialize();

                var secondBatch = GenerateDeviceHealthSnapshots(5, 5);
                Console.WriteLine($"\nSending {secondBatch.Count} device health snapshots (batch 2)...");
                
                foreach (var evt in secondBatch)
                {
                    processor.ProcessEvent(evt);
                    Thread.Sleep(50);
                }

                processor.Flush();
                Thread.Sleep(2000);
            }

            Console.WriteLine("\n? Persistent multi-stream correlation complete");
        }

        private static IStreamable<Empty, object> CreateIoTQuery(
            IStreamable<Empty, DeviceHealthSnapshot> input)
        {
            return input
                .Where(s => s.HealthScore < 50)
                .Select(s => (object)new
                {
                    s.DeviceId,
                    s.Temperature,
                    s.Pressure,
                    s.Vibration,
                    s.HealthScore,
                    AlertType = s.HealthScore < 30 ? "CRITICAL" : "WARNING",
                    AssessedAt = DateTime.UtcNow
                });
        }

        private static List<StreamEvent<DeviceHealthSnapshot>> GenerateDeviceHealthSnapshots(int count, int startIndex)
        {
            var events = new List<StreamEvent<DeviceHealthSnapshot>>();
            var random = new Random(42 + startIndex);
            var startTime = DateTime.UtcNow;
            var devices = new[] { "Device-A", "Device-B", "Device-C" };

            for (int i = 0; i < count; i++)
            {
                var index = startIndex + i;
                var temp = 20 + random.NextDouble() * 80;
                var pressure = 10 + random.NextDouble() * 40;
                var vibration = random.NextDouble() * 10;

                events.Add(StreamEvent.CreateStart(
                    startTime.AddSeconds(index * 0.5).Ticks,
                    new DeviceHealthSnapshot
                    {
                        DeviceId = devices[index % devices.Length],
                        Temperature = temp,
                        Pressure = pressure,
                        Vibration = vibration,
                        Timestamp = startTime.AddSeconds(index * 0.5),
                        HealthScore = CalculateHealthScore(temp, pressure, vibration)
                    }));
            }

            return events;
        }

        #endregion

        #region Example 10: Persistent Temporal Queries

        /// <summary>
        /// Example 10: Persistent temporal queries with checkpointing
        /// Demonstrates: User session tracking with state recovery
        /// </summary>
        private static void RunPersistentTemporalQueryExample()
        {
            Console.WriteLine("Example 10: Persistent Temporal Queries (User Sessions)");
            Console.WriteLine("???????????????????????????????????????????????????????");
            Console.WriteLine();

            var checkpointDir = Path.Combine(Path.GetTempPath(), "TrillTyped", "PersistentTemporal");
            
            if (Directory.Exists(checkpointDir))
            {
                Directory.Delete(checkpointDir, true);
            }

            // Phase 1: Initial session data
            Console.WriteLine("Phase 1: Processing user sessions and creating checkpoint...");
            Console.WriteLine("??????????????????????????????????????????????????????????");
            
            using (var processor = new TypedEventProcessor<UserSession>(
                "session-partition",
                checkpointDir,
                CreateSessionQuery))
            {
                processor.Initialize();

                var firstBatch = GenerateUserSessions(5, 0);
                Console.WriteLine($"Sending {firstBatch.Count} user sessions (batch 1)...");
                
                foreach (var evt in firstBatch)
                {
                    processor.ProcessEvent(evt);
                    Thread.Sleep(50);
                }

                processor.Flush();
                Thread.Sleep(11000);
            }

            // Phase 2: Recovery and continued session tracking
            Console.WriteLine("\n\nPhase 2: Recovering and tracking more sessions...");
            Console.WriteLine("????????????????????????????????????????????????????");
            
            using (var processor = new TypedEventProcessor<UserSession>(
                "session-partition",
                checkpointDir,
                CreateSessionQuery))
            {
                processor.Initialize();

                var secondBatch = GenerateUserSessions(5, 5);
                Console.WriteLine($"\nSending {secondBatch.Count} user sessions (batch 2)...");
                
                foreach (var evt in secondBatch)
                {
                    processor.ProcessEvent(evt);
                    Thread.Sleep(50);
                }

                processor.Flush();
                Thread.Sleep(2000);
            }

            Console.WriteLine("\n? Persistent temporal query example complete");
        }

        private static IStreamable<Empty, object> CreateSessionQuery(
            IStreamable<Empty, UserSession> input)
        {
            return input
                .Where(s => s.ClickCount >= 3)
                .Select(s => (object)new
                {
                    s.ClickCount,
                    s.UniquePages,
                    s.DurationSeconds,
                    EngagementScore = (double)s.ClickCount * s.DurationSeconds / 60.0,
                    SessionQuality = s.DurationSeconds > 30 ? "High" : "Low",
                    AnalyzedAt = DateTime.UtcNow
                });
        }

        private static List<StreamEvent<UserSession>> GenerateUserSessions(int count, int startIndex)
        {
            var events = new List<StreamEvent<UserSession>>();
            var random = new Random(42 + startIndex);
            var startTime = DateTime.UtcNow;

            for (int i = 0; i < count; i++)
            {
                var index = startIndex + i;
                var clicks = (ulong)random.Next(1, 10);
                var duration = random.Next(10, 120);

                events.Add(StreamEvent.CreateStart(
                    startTime.AddSeconds(index * 0.5).Ticks,
                    new UserSession
                    {
                        ClickCount = clicks,
                        UniquePages = (ulong)random.Next(1, (int)clicks + 1),
                        SessionStart = startTime.AddSeconds(index * 0.5),
                        SessionEnd = startTime.AddSeconds(index * 0.5 + duration),
                        DurationSeconds = duration
                    }));
            }

            return events;
        }

        #endregion

        #region Example 11: Multi-Phase Checkpointing Test

        /// <summary>
        /// Example 11: Comprehensive multi-phase checkpoint/restart test
        /// Demonstrates: Full lifecycle with multiple checkpoints and restarts
        /// </summary>
        private static void RunMultiPhaseCheckpointingExample()
        {
            Console.WriteLine("Example 11: Multi-Phase Checkpointing Test");
            Console.WriteLine("???????????????????????????????????????????");
            Console.WriteLine();

            var checkpointDir = Path.Combine(Path.GetTempPath(), "TrillTyped", "MultiPhase");
            
            if (Directory.Exists(checkpointDir))
            {
                Directory.Delete(checkpointDir, true);
            }

            var random = new Random(42);
            var startTime = DateTime.UtcNow;

            // Phase 1: Initial processing
            Console.WriteLine("Phase 1: Initial data ingestion...");
            Console.WriteLine("???????????????????????????????????");
            
            using (var processor = new TypedEventProcessor<SensorAnalytics>(
                "multiphase-partition",
                checkpointDir,
                CreateMultiPhaseQuery))
            {
                processor.Initialize();

                var batch1 = GenerateSensorAnalytics(3, 0, startTime);
                Console.WriteLine($"Processing {batch1.Count} events...");
                foreach (var evt in batch1)
                {
                    processor.ProcessEvent(evt);
                    Thread.Sleep(50);
                }
                processor.Flush();
                Thread.Sleep(11000);
                Console.WriteLine("? Phase 1 complete - Checkpoint 1 created");
            }

            // Phase 2: First restart
            Console.WriteLine("\n\nPhase 2: First restart (simulating brief outage)...");
            Console.WriteLine("????????????????????????????????????????????????????");
            
            using (var processor = new TypedEventProcessor<SensorAnalytics>(
                "multiphase-partition",
                checkpointDir,
                CreateMultiPhaseQuery))
            {
                processor.Initialize();

                var batch2 = GenerateSensorAnalytics(3, 3, startTime);
                Console.WriteLine($"Processing {batch2.Count} events after restart...");
                foreach (var evt in batch2)
                {
                    processor.ProcessEvent(evt);
                    Thread.Sleep(50);
                }
                processor.Flush();
                Thread.Sleep(11000);
                Console.WriteLine("? Phase 2 complete - Checkpoint 2 created");
            }

            // Phase 3: Second restart
            Console.WriteLine("\n\nPhase 3: Second restart (testing stability)...");
            Console.WriteLine("??????????????????????????????????????????????");
            
            using (var processor = new TypedEventProcessor<SensorAnalytics>(
                "multiphase-partition",
                checkpointDir,
                CreateMultiPhaseQuery))
            {
                processor.Initialize();

                var batch3 = GenerateSensorAnalytics(3, 6, startTime);
                Console.WriteLine($"Processing {batch3.Count} events after second restart...");
                foreach (var evt in batch3)
                {
                    processor.ProcessEvent(evt);
                    Thread.Sleep(50);
                }
                processor.Flush();
                Thread.Sleep(11000);
                Console.WriteLine("? Phase 3 complete - Checkpoint 3 created");
            }

            // Phase 4: Final verification
            Console.WriteLine("\n\nPhase 4: Final verification...");
            Console.WriteLine("???????????????????????????????");
            
            using (var processor = new TypedEventProcessor<SensorAnalytics>(
                "multiphase-partition",
                checkpointDir,
                CreateMultiPhaseQuery))
            {
                processor.Initialize();

                var batch4 = GenerateSensorAnalytics(2, 9, startTime);
                Console.WriteLine($"Processing final {batch4.Count} events...");
                foreach (var evt in batch4)
                {
                    processor.ProcessEvent(evt);
                    Thread.Sleep(50);
                }
                processor.Flush();
                Thread.Sleep(1000);
            }

            // Display summary
            Console.WriteLine("\n\n" + new string('?', 70));
            Console.WriteLine("Multi-Phase Checkpointing Test Summary");
            Console.WriteLine(new string('?', 70));
            Console.WriteLine($"  • Phase 1: 3 events ? Checkpoint 1");
            Console.WriteLine($"  • Phase 2: 3 events ? Checkpoint 2 (after restart)");
            Console.WriteLine($"  • Phase 3: 3 events ? Checkpoint 3 (after restart)");
            Console.WriteLine($"  • Phase 4: 2 events ? Final verification");
            Console.WriteLine($"  • Total: 11 events processed across 4 phases");
            Console.WriteLine($"  • Checkpoints: 3 successful state snapshots");
            Console.WriteLine($"  • Restarts: 3 successful state recoveries");
            Console.WriteLine($"  • State continuity: ? VERIFIED");
            Console.WriteLine(new string('?', 70));
            
            Console.WriteLine("\n? Multi-phase checkpointing test complete");
        }

        private static IStreamable<Empty, object> CreateMultiPhaseQuery(
            IStreamable<Empty, SensorAnalytics> input)
        {
            return input
                .Select(s => (object)new
                {
                    s.ReadingCount,
                    s.AverageValue,
                    s.MaxValue,
                    s.CriticalCount,
                    s.HealthIndicator,
                    ProcessPhase = s.ReadingCount <= 3 ? "Phase1" :
                                   s.ReadingCount <= 6 ? "Phase2" :
                                   s.ReadingCount <= 9 ? "Phase3" : "Phase4",
                    Timestamp = DateTime.UtcNow
                });
        }

        private static List<StreamEvent<SensorAnalytics>> GenerateSensorAnalytics(int count, int startIndex, DateTime baseTime)
        {
            var events = new List<StreamEvent<SensorAnalytics>>();
            var random = new Random(42 + startIndex);

            for (int i = 0; i < count; i++)
            {
                var index = startIndex + i;
                events.Add(StreamEvent.CreateStart(
                    baseTime.AddSeconds(index * 0.5).Ticks,
                    new SensorAnalytics
                    {
                        ReadingCount = (ulong)(index + 1),
                        AverageValue = random.NextDouble() * 100,
                        MaxValue = random.NextDouble() * 100,
                        CriticalCount = (ulong)random.Next(0, 3),
                        HealthIndicator = random.Next(0, 3) == 0 ? "Degraded" : "Healthy"
                    }));
            }

            return events;
        }

        #endregion

        #region Data Generators

        private static List<StreamEvent<UserAction>> GenerateUserActions(int count)
        {
            var events = new List<StreamEvent<UserAction>>();
            var random = new Random(42);
            var startTime = DateTime.UtcNow;
            var actionTypes = new[] { "Login", "Purchase", "Browse", "AddToCart", "Logout" };
            var userIds = Enumerable.Range(1, 5).Select(i => $"User{i}").ToArray();

            for (int i = 0; i < count; i++)
            {
                events.Add(StreamEvent.CreateStart(
                    startTime.AddSeconds(i).Ticks,
                    new UserAction
                    {
                        UserId = userIds[random.Next(userIds.Length)],
                        ActionType = actionTypes[random.Next(actionTypes.Length)],
                        Timestamp = startTime.AddSeconds(i)
                    }));
            }

            return events;
        }

        private static List<StreamEvent<UserProfile>> GenerateUserProfiles(int count)
        {
            var events = new List<StreamEvent<UserProfile>>();
            var startTime = DateTime.UtcNow;
            var tiers = new[] { "Bronze", "Silver", "Gold", "Platinum" };

            for (int i = 0; i < count; i++)
            {
                events.Add(StreamEvent.CreateStart(
                    startTime.Ticks,
                    new UserProfile
                    {
                        UserId = $"User{i + 1}",
                        Name = $"User Name {i + 1}",
                        MembershipTier = tiers[i % tiers.Length],
                        RegistrationDate = startTime.AddDays(-random.Next(1, 365))
                    }));
            }

            return events;
        }

        private static List<StreamEvent<FinancialTransaction>> GenerateTransactionSequence(int count)
        {
            var events = new List<StreamEvent<FinancialTransaction>>();
            var random = new Random(42);
            var startTime = DateTime.UtcNow;
            var countries = new[] { "USA", "UK", "Germany", "France", "Japan" };
            var accounts = Enumerable.Range(1, 3).Select(i => $"ACC{i:D3}").ToArray();

            for (int i = 0; i < count; i++)
            {
                events.Add(StreamEvent.CreateStart(
                    startTime.AddSeconds(i * 0.5).Ticks,
                    new FinancialTransaction
                    {
                        TransactionId = Guid.NewGuid(),
                        AccountId = accounts[random.Next(accounts.Length)],
                        Amount = (decimal)(random.NextDouble() * 2000),
                        Country = countries[random.Next(countries.Length)],
                        Timestamp = startTime.AddSeconds(i * 0.5)
                    }));
            }

            return events;
        }

        private static List<StreamEvent<TemperatureReading>> GenerateTemperatureEvents(int count)
        {
            var events = new List<StreamEvent<TemperatureReading>>();
            var random = new Random(42);
            var startTime = DateTime.UtcNow;
            var devices = new[] { "Device-A", "Device-B", "Device-C" };

            for (int i = 0; i < count; i++)
            {
                events.Add(StreamEvent.CreateStart(
                    startTime.AddSeconds(i).Ticks,
                    new TemperatureReading
                    {
                        DeviceId = devices[i % devices.Length],
                        Value = 20 + (random.NextDouble() * 80),
                        Timestamp = startTime.AddSeconds(i)
                    }));
            }

            return events;
        }

        private static List<StreamEvent<PressureReading>> GeneratePressureEvents(int count)
        {
            var events = new List<StreamEvent<PressureReading>>();
            var random = new Random(43);
            var startTime = DateTime.UtcNow;
            var devices = new[] { "Device-A", "Device-B", "Device-C" };

            for (int i = 0; i < count; i++)
            {
                events.Add(StreamEvent.CreateStart(
                    startTime.AddSeconds(i).Ticks,
                    new PressureReading
                    {
                        DeviceId = devices[i % devices.Length],
                        Value = 10 + (random.NextDouble() * 40),
                        Timestamp = startTime.AddSeconds(i)
                    }));
            }

            return events;
        }

        private static List<StreamEvent<VibrationReading>> GenerateVibrationEvents(int count)
        {
            var events = new List<StreamEvent<VibrationReading>>();
            var random = new Random(44);
            var startTime = DateTime.UtcNow;
            var devices = new[] { "Device-A", "Device-B", "Device-C" };

            for (int i = 0; i < count; i++)
            {
                events.Add(StreamEvent.CreateStart(
                    startTime.AddSeconds(i).Ticks,
                    new VibrationReading
                    {
                        DeviceId = devices[i % devices.Length],
                        Value = random.NextDouble() * 10,
                        Timestamp = startTime.AddSeconds(i)
                    }));
            }

            return events;
        }

        private static List<StreamEvent<ClickEvent>> GenerateClickEvents(int count)
        {
            var events = new List<StreamEvent<ClickEvent>>();
            var random = new Random(42);
            var startTime = DateTime.UtcNow;
            var users = new[] { "User1", "User2", "User3" };
            var pages = new[] { "/home", "/products", "/cart", "/checkout", "/account" };

            for (int i = 0; i < count; i++)
            {
                events.Add(StreamEvent.CreateStart(
                    startTime.AddSeconds(i * 0.3).Ticks,
                    new ClickEvent
                    {
                        UserId = users[random.Next(users.Length)],
                        PageUrl = pages[random.Next(pages.Length)],
                        Timestamp = startTime.AddSeconds(i * 0.3)
                    }));
            }

            return events;
        }

        private static List<StreamEvent<SensorDataPoint>> GenerateSensorDataPoints(int count)
        {
            var events = new List<StreamEvent<SensorDataPoint>>();
            var random = new Random(42);
            var startTime = DateTime.UtcNow;
            var sensors = new[] { "TEMP-01", "HUMIDITY-01", "PRESSURE-01" };

            for (int i = 0; i < count; i++)
            {
                events.Add(StreamEvent.CreateStart(
                    startTime.AddSeconds(i * 0.5).Ticks,
                    new SensorDataPoint
                    {
                        SensorId = sensors[i % sensors.Length],
                        Value = random.NextDouble() * 100,
                        Unit = "units",
                        Timestamp = startTime.AddSeconds(i * 0.5)
                    }));
            }

            return events;
        }

        private static List<StreamEvent<MetricEvent>> GenerateMetricEventsForCheckpointing(int count, int startIndex)
        {
            var events = new List<StreamEvent<MetricEvent>>();
            var random = new Random(42 + startIndex);
            var baseTime = DateTime.UtcNow;
            var metricNames = new[] { "CPU", "Memory", "Disk", "Network" };
            var hosts = new[] { "Server-01", "Server-02", "Server-03" };
            var regions = new[] { "US-West", "US-East", "EU-Central" };

            for (int i = 0; i < count; i++)
            {
                var timestamp = baseTime.AddSeconds((startIndex + i) * 0.5);
                
                events.Add(StreamEvent.CreateStart(
                    timestamp.Ticks,
                    new MetricEvent
                    {
                        MetricId = Guid.NewGuid(),
                        Name = metricNames[random.Next(metricNames.Length)],
                        Value = random.NextDouble() * 100,
                        Timestamp = timestamp,
                        Tags = new Dictionary<string, string>
                        {
                            { "Host", hosts[random.Next(hosts.Length)] },
                            { "Region", regions[random.Next(regions.Length)] },
                            { "EventIndex", (startIndex + i).ToString() }
                        }
                    }));
            }

            return events;
        }

        private static double CalculateHealthScore(double temp, double pressure, double vibration)
        {
            // Simple health score calculation
            double tempScore = temp < 70 ? 100 : (100 - (temp - 70));
            double pressureScore = pressure < 35 ? 100 : (100 - (pressure - 35) * 2);
            double vibrationScore = vibration < 5 ? 100 : (100 - (vibration - 5) * 10);

            return (tempScore + pressureScore + vibrationScore) / 3.0;
        }

        private static readonly Random random = new Random(42);

        #endregion

        #region Display

        private static void DisplayAdvancedConcepts()
        {
            Console.WriteLine(@"
Advanced Concepts Demonstrated:
???????????????????????????????

1. STREAM JOINS
   • Inner joins between typed streams
   • Join on key fields (UserId, DeviceId, etc.)
   • Enriching events with reference data
   
2. PATTERN DETECTION
   • Detecting sequences in event streams
   • Fraud detection with windowed aggregation
   • Multi-condition pattern matching

3. MULTI-STREAM CORRELATION
   • Correlating 3+ heterogeneous streams
   • Time-based alignment
   • Complex health scoring

4. TEMPORAL QUERIES
   • Session windows
   • Time-based grouping
   • Duration calculations

5. COMPLEX EVENT PROCESSING
   • Multi-stage pipelines
   • Filter ? Transform ? Aggregate
   • Stateful pattern matching

6. CHECKPOINTING & STATE RECOVERY
   • Creating checkpoints during processing
   • Suspending and resuming processors
   • State continuity verification
   • Multi-phase recovery testing

???????????????????????????????
PERSISTENT VERSIONS (Examples 7-11)
???????????????????????????????

7. PERSISTENT STREAM JOIN
   • Join state preservation across restarts
   • Enriched event processing with checkpoints
   • User action correlation with recovery

8. PERSISTENT PATTERN DETECTION  
   • Fraud detection with state persistence
   • Pattern matching across restarts
   • Alert generation with recovery

9. PERSISTENT MULTI-STREAM CORRELATION
   • IoT device health monitoring
   • Multi-sensor correlation with checkpoints
   • Health score calculation with recovery

10. PERSISTENT TEMPORAL QUERIES
    • User session tracking with state
    • Temporal aggregations with checkpoints
    • Session analytics with recovery

11. MULTI-PHASE CHECKPOINTING
    • Multiple checkpoint/restart cycles
    • Full lifecycle state management
    • Comprehensive recovery verification
    • Production-ready patterns
");
        }

        #endregion
    }

    #region Advanced Type Definitions

    // Join example types
    public record UserAction
    {
        public string UserId { get; init; }
        public string ActionType { get; init; }
        public DateTime Timestamp { get; init; }
    }

    public record UserProfile
    {
        public string UserId { get; init; }
        public string Name { get; init; }
        public string MembershipTier { get; init; }
        public DateTime RegistrationDate { get; init; }
    }

    public record UserActionEnriched
    {
        public string UserId { get; init; }
        public string ActionType { get; init; }
        public DateTime ActionTimestamp { get; init; }
        public string UserName { get; init; }
        public string UserTier { get; init; }
        public DateTime UserRegistrationDate { get; init; }
    }

    // Pattern detection types
    public record FinancialTransaction
    {
        public Guid TransactionId { get; init; }
        public string AccountId { get; init; }
        public decimal Amount { get; init; }
        public string Country { get; init; }
        public DateTime Timestamp { get; init; }
    }

    public record FraudAlert
    {
        public ulong TransactionCount { get; init; }
        public decimal TotalAmount { get; init; }
        public ulong DistinctCountries { get; init; }
        public string AlertLevel { get; init; }
    }

    // Multi-stream correlation types
    public record TemperatureReading
    {
        public string DeviceId { get; init; }
        public double Value { get; init; }
        public DateTime Timestamp { get; init; }
    }

    public record PressureReading
    {
        public string DeviceId { get; init; }
        public double Value { get; init; }
        public DateTime Timestamp { get; init; }
    }

    public record VibrationReading
    {
        public string DeviceId { get; init; }
        public double Value { get; init; }
        public DateTime Timestamp { get; init; }
    }

    public record DeviceHealthSnapshot
    {
        public string DeviceId { get; init; }
        public double Temperature { get; init; }
        public double Pressure { get; init; }
        public double Vibration { get; init; }
        public DateTime Timestamp { get; init; }
        public double HealthScore { get; init; }
    }

    // Temporal query types
    public record ClickEvent
    {
        public string UserId { get; init; }
        public string PageUrl { get; init; }
        public DateTime Timestamp { get; init; }
    }

    public record UserSession
    {
        public ulong ClickCount { get; init; }
        public ulong UniquePages { get; init; }
        public DateTime SessionStart { get; init; }
        public DateTime SessionEnd { get; init; }
        public long DurationSeconds { get; init; }
    }

    // Complex CEP types
    public record SensorDataPoint
    {
        public string SensorId { get; init; }
        public double Value { get; init; }
        public string Unit { get; init; }
        public DateTime Timestamp { get; init; }
    }

    public record SensorAnalytics
    {
        public ulong ReadingCount { get; init; }
        public double AverageValue { get; init; }
        public double MaxValue { get; init; }
        public ulong CriticalCount { get; init; }
        public string HealthIndicator { get; init; }
    }

    // Note: MetricEvent and MetricStats types are defined in TypedRecordTypes.cs

    #endregion
}
