// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.StreamProcessing;

namespace EventHubReceiver
{
    /// <summary>
    /// Local receiver that demonstrates flexible event processing with Trill
    /// Uses FlexiblePayload to provide dynamic-like behavior while working with Trill
    /// </summary>
    public static class DynamicReceiver
    {
        private const string CheckpointDirectory = "./dynamic_checkpoints";

        public static async Task RunAsync()
        {
            Console.WriteLine("??????????????????????????????????????????????????????????????");
            Console.WriteLine("?      Dynamic Event Processor - Configuration              ?");
            Console.WriteLine("??????????????????????????????????????????????????????????????");
            Console.WriteLine();

            // Select aggregation mode
            var mode = SelectAggregationMode();

            Console.WriteLine();
            Console.Write("Enter window size in seconds [default: 5]: ");
            var windowInput = Console.ReadLine();
            var windowSize = string.IsNullOrWhiteSpace(windowInput)
                ? TimeSpan.FromSeconds(5)
                : TimeSpan.FromSeconds(double.Parse(windowInput));

            Console.Write("Enter slide size in seconds [default: 1]: ");
            var slideInput = Console.ReadLine();
            var slideSize = string.IsNullOrWhiteSpace(slideInput)
                ? TimeSpan.FromSeconds(1)
                : TimeSpan.FromSeconds(double.Parse(slideInput));

            Console.Write("Enter number of events to generate [default: 100]: ");
            var countInput = Console.ReadLine();
            var eventCount = string.IsNullOrWhiteSpace(countInput)
                ? 100
                : int.Parse(countInput);

            Console.Write("Enter event generation rate (ms delay) [default: 100]: ");
            var rateInput = Console.ReadLine();
            var generationRate = string.IsNullOrWhiteSpace(rateInput)
                ? 100
                : int.Parse(rateInput);

            Console.WriteLine();
            Console.WriteLine("Starting Dynamic Event Processing...");
            Console.WriteLine($"Mode: {mode}");
            Console.WriteLine($"Window: {windowSize.TotalSeconds}s, Slide: {slideSize.TotalSeconds}s");
            Console.WriteLine($"Events: {eventCount}, Rate: {generationRate}ms");
            Console.WriteLine();

            var config = DynamicAggregationConfig.Create(mode, windowSize, slideSize);
            await ProcessDynamicEventsAsync(config, eventCount, generationRate);

            Console.WriteLine();
            Console.WriteLine("Press any key to return to menu...");
            Console.ReadKey();
        }

        private static AggregationMode SelectAggregationMode()
        {
            Console.WriteLine("Select Aggregation Mode:");
            Console.WriteLine("  1. Simple Stats (Count only)");
            Console.WriteLine("  2. Custom Fields (Sum, Average from 'Value' field)");
            Console.WriteLine("  3. Multi-Metric (Count, Sum, Avg, Min, Max from 'Size' field)");
            Console.WriteLine();
            Console.Write("Enter choice [default: 3]: ");

            var choice = Console.ReadLine()?.Trim();
            if (string.IsNullOrWhiteSpace(choice))
            {
                choice = "3";
            }

            return choice switch
            {
                "1" => AggregationMode.SimpleStats,
                "2" => AggregationMode.CustomFields,
                "3" => AggregationMode.MultiMetric,
                _ => AggregationMode.MultiMetric
            };
        }

        private static async Task ProcessDynamicEventsAsync(
            DynamicAggregationConfig config,
            int eventCount,
            int generationRate)
        {
            using (var processor = new DynamicEventProcessor("partition-0", CheckpointDirectory, config))
            {
                processor.Initialize();

                var random = new Random();
                var baseTime = DateTime.UtcNow.Ticks;
                var cts = new CancellationTokenSource();

                Console.WriteLine("Processing started. Press 'Q' to stop early...");
                Console.WriteLine();

                // Start a task to monitor for quit key
                var quitTask = Task.Run(() =>
                {
                    while (!cts.Token.IsCancellationRequested)
                    {
                        if (Console.KeyAvailable && Console.ReadKey(true).Key == ConsoleKey.Q)
                        {
                            cts.Cancel();
                            break;
                        }

                        Thread.Sleep(100);
                    }
                });

                try
                {
                    for (int i = 0; i < eventCount && !cts.Token.IsCancellationRequested; i++)
                    {
                        // Create flexible payload based on aggregation mode
                        var flexiblePayload = CreateFlexiblePayload(config.AggregationMode, random, i);

                        var eventTime = baseTime + TimeSpan.FromMilliseconds(i * generationRate).Ticks;
                        var streamEvent = StreamEvent.CreateStart(eventTime, flexiblePayload);

                        processor.ProcessEvent(streamEvent);

                        // Display progress
                        if ((i + 1) % 10 == 0)
                        {
                            Console.WriteLine($"Generated {i + 1} events...");
                        }

                        await Task.Delay(generationRate, cts.Token);
                    }

                    // Send final punctuation to flush results
                    var finalTime = baseTime + TimeSpan.FromMilliseconds((eventCount + 1) * generationRate).Ticks;
                    processor.ProcessEvent(StreamEvent.CreatePunctuation<FlexiblePayload>(finalTime));
                    processor.Flush();

                    // Give some time for final results to be displayed
                    await Task.Delay(1000);
                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine();
                    Console.WriteLine("Processing stopped by user.");

                    // Send punctuation to flush any pending results
                    var finalTime = baseTime + TimeSpan.FromMilliseconds(eventCount * generationRate).Ticks;
                    processor.ProcessEvent(StreamEvent.CreatePunctuation<FlexiblePayload>(finalTime));
                    processor.Flush();
                }
                finally
                {
                    cts.Cancel();
                    try
                    {
                        await quitTask;
                    }
                    catch
                    {
                    }
                }

                Console.WriteLine();
                Console.WriteLine("Processing completed.");
            }
        }

        /// <summary>
        /// Create flexible payload based on aggregation mode
        /// Uses ExpandoObject for construction then converts to FlexiblePayload
        /// </summary>
        private static FlexiblePayload CreateFlexiblePayload(AggregationMode mode, Random random, int index)
        {
            dynamic expando = new ExpandoObject();
            var dict = expando as IDictionary<string, object>;

            // Common fields
            dict["EventId"] = index;
            dict["Timestamp"] = DateTime.UtcNow;

            switch (mode)
            {
                case AggregationMode.SimpleStats:
                    // Just needs to exist for counting
                    dict["Type"] = "SimpleEvent";
                    break;

                case AggregationMode.CustomFields:
                    // Add 'Value' field for custom aggregation
                    dict["Value"] = (long)(random.NextDouble() * 100);
                    dict["Category"] = $"Category{random.Next(1, 4)}";
                    break;

                case AggregationMode.MultiMetric:
                    // Add 'Size' field for multi-metric aggregation
                    dict["Size"] = (long)random.Next(100, 10000);
                    dict["Source"] = $"Source{random.Next(1, 6)}";
                    dict["Priority"] = random.Next(1, 11);
                    break;
            }

            return FlexiblePayload.FromExpandoObject(expando);
        }
    }
}
