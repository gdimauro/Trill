// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.StreamProcessing;
using SystemDiagnosticsProcess = System.Diagnostics.Process;

namespace EventHubSender
{
    /// <summary>
    /// Local on-premises sender and receiver without Azure dependencies
    /// </summary>
    public sealed class LocalSenderReceiver
    {
        private static LocalEventReceiver receiver;
        private static CancellationTokenSource cancellationTokenSource;
        private static readonly string DefaultPartitionId = "partition-0";

        public static async Task RunAsync()
        {
            Console.Clear();
            Console.WriteLine("=== Local On-Premises Event Processing ===");
            Console.WriteLine("This mode processes events locally without Azure dependencies.");
            Console.WriteLine();

            // Initialize receiver
            receiver = new LocalEventReceiver();
            receiver.Start();

            cancellationTokenSource = new CancellationTokenSource();

            Console.WriteLine("Starting local event sender and processor...");
            Console.WriteLine("Press Ctrl+C to stop.");
            Console.WriteLine();

            // Start sending events
            await SendLocalEventsAsync(cancellationTokenSource.Token);

            // Cleanup
            receiver.Stop();

            Console.WriteLine();
            Console.WriteLine("Local processing stopped.");
            Console.WriteLine("Press any key to return to menu.");
            Console.ReadLine();
        }

        /// <summary>
        /// Send events to the local processor
        /// </summary>
        private static async Task SendLocalEventsAsync(CancellationToken cancellationToken)
        {
            var proc = SystemDiagnosticsProcess.GetCurrentProcess();
            int messageCount = 0;

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        var message = StreamEvent.CreateStart(DateTime.UtcNow.Ticks, proc.WorkingSet64);

                        Console.WriteLine($"Processing event #{++messageCount}: WorkingSet={proc.WorkingSet64 / 1024 / 1024} MB");

                        // Process the event locally
                        receiver.ProcessEvent(DefaultPartitionId, message);

                        await Task.Delay(1000, cancellationToken);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                    catch (Exception exception)
                    {
                        Console.WriteLine($"{DateTime.Now} > Exception: {exception.Message}");
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
                receiver?.FlushAll();
            }
        }

        /// <summary>
        /// Stop the local sender/receiver
        /// </summary>
        public static void Stop()
        {
            cancellationTokenSource?.Cancel();
        }
    }
}
