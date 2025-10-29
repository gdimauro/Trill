// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Threading.Tasks;

namespace EventHubSender
{
    public sealed class Program
    {
        public static void Main(string[] args)
        {
            MainAsync(args).GetAwaiter().GetResult();
        }

        private static async Task MainAsync(string[] args)
        {
            bool exit = false;

            while (!exit)
            {
                Console.Clear();
                Console.WriteLine("╔════════════════════════════════════════════════════════════╗");
                Console.WriteLine("║        Trill Event Processing - Mode Selection            ║");
                Console.WriteLine("╚════════════════════════════════════════════════════════════╝");
                Console.WriteLine();
                Console.WriteLine("Choose execution mode:");
                Console.WriteLine();
                Console.WriteLine("  1. Azure Event Hub Mode (original)");
                Console.WriteLine("     - Requires Azure Event Hub connection");
                Console.WriteLine("     - Uses Azure storage for checkpoints");
                Console.WriteLine();
                Console.WriteLine("  2. Local On-Premises Mode (new)");
                Console.WriteLine("     - No Azure dependencies");
                Console.WriteLine("     - Local filesystem checkpointing");
                Console.WriteLine("     - Self-contained event processing");
                Console.WriteLine();
                Console.WriteLine("  3. Exit");
                Console.WriteLine();
                Console.Write("Enter your choice (1-3): ");

                var choice = Console.ReadLine()?.Trim();

                try
                {
                    switch (choice)
                    {
                        case "1":
                            Console.WriteLine();
                            Console.WriteLine("Starting Azure Event Hub mode...");
                            Console.WriteLine();
                            await AzureEventHubSender.RunAsync();
                            break;

                        case "2":
                            Console.WriteLine();
                            Console.WriteLine("Starting Local On-Premises mode...");
                            Console.WriteLine();
                            await LocalSenderReceiver.RunAsync();
                            break;

                        case "3":
                            exit = true;
                            Console.WriteLine();
                            Console.WriteLine("Exiting application. Goodbye!");
                            break;

                        default:
                            Console.WriteLine();
                            Console.WriteLine("Invalid choice. Press any key to try again...");
                            Console.ReadKey();
                            break;
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine();
                    Console.WriteLine($"Error: {ex.Message}");
                    Console.WriteLine();
                    Console.WriteLine("Press any key to return to menu...");
                    Console.ReadKey();
                }
            }
        }
    }
}
