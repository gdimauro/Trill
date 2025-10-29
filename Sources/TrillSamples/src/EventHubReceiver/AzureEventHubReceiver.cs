// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.EventHubs.Processor;

namespace EventHubReceiver
{
    /// <summary>
    /// Original Azure Event Hub receiver logic
    /// </summary>
    public sealed class AzureEventHubReceiver
    {
        private const string EventHubConnectionString = "<fill>";
        private const string EventHubName = "<fill>";
        private const string StorageContainerName = "<fill>";
        private const string StorageAccountName = "<fill>";
        private const string StorageAccountKey = "<fill>";

        internal static readonly string StorageConnectionString =
            $"DefaultEndpointsProtocol=https;AccountName={StorageAccountName};AccountKey={StorageAccountKey}";

        public static async Task RunAsync()
        {
            Console.Clear();
            Console.WriteLine("???????????????????????????????????????????????????????????");
            Console.WriteLine("  Trill Azure Event Hub Receiver");
            Console.WriteLine("???????????????????????????????????????????????????????????");
            Console.WriteLine();
            Console.WriteLine("Registering EventProcessor...");

            var eventProcessorHost = new EventProcessorHost(
                EventHubName,
                PartitionReceiver.DefaultConsumerGroupName,
                EventHubConnectionString,
                StorageConnectionString,
                StorageContainerName);

            // Registers the Event Processor Host and starts receiving messages
            await eventProcessorHost.RegisterEventProcessorAsync<EventProcessor>();

            Console.WriteLine();
            Console.WriteLine("Receiving from Azure Event Hub...");
            Console.WriteLine("Press Enter to stop receiver.");
            Console.ReadLine();

            // Disposes of the Event Processor Host
            await eventProcessorHost.UnregisterEventProcessorAsync();

            Console.WriteLine();
            Console.WriteLine("Azure Event Hub receiver stopped.");
            Console.WriteLine("Press any key to return to menu.");
            Console.ReadLine();
        }
    }
}
