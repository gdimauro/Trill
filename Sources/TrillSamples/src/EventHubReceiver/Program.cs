// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Threading.Tasks;
using EventHubReceiver.Kql;

namespace EventHubReceiver
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
        Console.WriteLine("║      Trill Event Receiver - Mode Selection                ║");
        Console.WriteLine("╚════════════════════════════════════════════════════════════╝");
        Console.WriteLine();
        Console.WriteLine("Choose receiver mode:");
        Console.WriteLine();
        Console.WriteLine("  1. Local On-Premises Mode (default)");
        Console.WriteLine("     - No Azure dependencies");
        Console.WriteLine("     - Local filesystem checkpointing");
        Console.WriteLine("     - Simulated event processing");
        Console.WriteLine();
        Console.WriteLine("  2. Azure Event Hub Mode (original)");
        Console.WriteLine("     - Requires Azure Event Hub connection");
        Console.WriteLine("     - Uses Azure storage for checkpoints");
        Console.WriteLine();
        Console.WriteLine("  3. Dynamic Event Processing Mode");
        Console.WriteLine("     - Uses dynamic objects (ExpandoObject)");
        Console.WriteLine("     - Flexible, decoupled aggregation logic");
        Console.WriteLine("     - Runtime-configurable aggregations");
        Console.WriteLine();
        Console.WriteLine("  4. KQL Schema-Based Processing Mode");
        Console.WriteLine("     - Define schemas using KQL syntax");
        Console.WriteLine("     - Type-safe dynamic event processing");
        Console.WriteLine("     - Full checkpointing support");
        Console.WriteLine();
        Console.WriteLine("  5. KQL Tests & Validation");
        Console.WriteLine("     - Run comprehensive test suite");
        Console.WriteLine("     - Validate schema parsing and processing");
        Console.WriteLine();
        Console.WriteLine("  6. Exit");
        Console.WriteLine();
        Console.Write("Enter your choice (1-6) [default: 1]: ");

        var choice = Console.ReadLine()?.Trim();

        // Default to local mode if empty
        if (string.IsNullOrWhiteSpace(choice))
        {
          choice = "1";
        }

        try
        {
          switch (choice)
          {
            case "1":
              Console.WriteLine();
              Console.WriteLine("Starting Local On-Premises mode...");
              Console.WriteLine();
              await LocalReceiver.RunAsync();
              break;

            case "2":
              Console.WriteLine();
              Console.WriteLine("Starting Azure Event Hub mode...");
              Console.WriteLine();
              await AzureEventHubReceiver.RunAsync();
              break;

            case "3":
              Console.WriteLine();
              Console.WriteLine("Starting Dynamic Event Processing mode...");
              Console.WriteLine();
              await DynamicReceiver.RunAsync();
              break;

            case "4":
              Console.WriteLine();
              Console.WriteLine("Starting KQL Schema-Based Processing mode...");
              Console.WriteLine();
              KqlProcessorSample.Run();
              Console.WriteLine();
              Console.WriteLine("Press any key to return to menu...");
              Console.ReadKey();
              break;

            case "5":
              Console.WriteLine();
              Console.WriteLine("Running KQL Tests...");
              Console.WriteLine();
              KqlTests.RunAllTests();
              Console.WriteLine();
              Console.WriteLine("Press any key to return to menu...");
              Console.ReadKey();
              break;

            case "6":
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
          Console.WriteLine("Stack trace:");
          Console.WriteLine(ex.StackTrace);
          Console.WriteLine();
          Console.WriteLine("Press any key to return to menu...");
          Console.ReadKey();
        }
      }
    }
  }
}
