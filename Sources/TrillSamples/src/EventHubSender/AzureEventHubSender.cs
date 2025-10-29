// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.StreamProcessing;
using SystemDiagnosticsProcess = System.Diagnostics.Process;

namespace EventHubSender
{
    /// <summary>
    /// Original Azure Event Hub sender logic
    /// </summary>
    public sealed class AzureEventHubSender
    {
        private const string EventHubConnectionString = "<fill>";
        private const string EventHubName = "<fill>";

        private static EventHubClient eventHubClient;

        public static async Task RunAsync()
        {
            var connectionStringBuilder = new EventHubsConnectionStringBuilder(EventHubConnectionString)
            {
                EntityPath = EventHubName
            };

            eventHubClient = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());

            Console.WriteLine("Starting Azure Event Hub sender...");
            Console.WriteLine("Press Ctrl+C to stop.");
            Console.WriteLine();

            await SendMessagesToEventHub(100);

            await eventHubClient.CloseAsync();

            Console.WriteLine("Press any key to return to menu.");
            Console.ReadLine();
        }

        // Creates an Event Hub client and sends messages to the event hub.
        private static async Task SendMessagesToEventHub(int numMessagesToSend)
        {
            var proc = SystemDiagnosticsProcess.GetCurrentProcess();

            int messageCount = 0;
            while (true)
            {
                try
                {
                    var message = StreamEvent.CreateStart(DateTime.UtcNow.Ticks, proc.WorkingSet64);
                    Console.WriteLine($"Sending message #{++messageCount}: {message}");
                    await eventHubClient.SendAsync(new EventData(BinarySerializer.Serialize(message)), "default");
                }
                catch (Exception exception)
                {
                    Console.WriteLine($"{DateTime.Now} > Exception: {exception.Message}");
                }
                await Task.Delay(1000);
            }
        }
    }
}
