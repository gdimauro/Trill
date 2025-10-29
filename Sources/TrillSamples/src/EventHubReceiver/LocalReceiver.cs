// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.StreamProcessing;
using SystemDiagnosticsProcess = System.Diagnostics.Process;

namespace EventHubReceiver
{
    /// <summary>
    /// Local receiver that runs the LocalEventProcessor for testing/development
    /// </summary>
    public sealed class LocalReceiver
    {
        private static LocalEventProcessor processor;
        private static CancellationTokenSource cancellationTokenSource;
        private static readonly string DefaultPartitionId = "partition-0";
        private static readonly string DefaultCheckpointDirectory = Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
            "TrillCheckpoints");

        public static async Task RunAsync()
        {
            Console.Clear();
            Console.WriteLine("???????????????????????????????????????????????????????????");
            Console.WriteLine("  Trill Local Event Receiver - On-Premises Mode");
            Console.WriteLine("???????????????????????????????????????????????????????????");
            Console.WriteLine();
            Console.WriteLine($"Checkpoint directory: {DefaultCheckpointDirectory}");
            Console.WriteLine();

            // Initialize processor
            processor = new LocalEventProcessor(DefaultPartitionId, DefaultCheckpointDirectory);
            processor.Initialize();

            cancellationTokenSource = new CancellationTokenSource();

            Console.WriteLine("Local Event Receiver started");
            Console.WriteLine("Processing events from simulated source...");
            Console.WriteLine("Press Ctrl+C to stop.");
            Console.WriteLine();

            // Start processing simulated events
            await ProcessSimulatedEventsAsync(cancellationTokenSource.Token);

            // Cleanup
            processor.Dispose();

            Console.WriteLine();
            Console.WriteLine("Local processing stopped.");
            Console.WriteLine("Press any key to exit.");
            Console.ReadLine();
        }

        /// <summary>
        /// Generate and process simulated events
        /// </summary>
        private static async Task ProcessSimulatedEventsAsync(CancellationToken cancellationToken)
        {
            var proc = SystemDiagnosticsProcess.GetCurrentProcess();
            int eventCount = 0;

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        // Create event with current process working set
                        var evt = StreamEvent.CreateStart(DateTime.UtcNow.Ticks, proc.WorkingSet64);

                        eventCount++;
                        if (eventCount % 10 == 0)
                        {
                            Console.WriteLine($"Processed {eventCount} events (WorkingSet: {proc.WorkingSet64 / 1024 / 1024} MB)");
                        }

                        // Process the event
                        processor.ProcessEvent(evt);

                        await Task.Delay(1000, cancellationToken);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"{DateTime.Now} > Error processing event: {ex.Message}");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Normal shutdown
            }
            finally
            {
                // Flush any remaining events
                Console.WriteLine($"Total events processed: {eventCount}");
                processor?.Flush();
            }
        }

        /// <summary>
        /// Stop the receiver
        /// </summary>
        public static void Stop()
        {
            cancellationTokenSource?.Cancel();
        }
    }
}
