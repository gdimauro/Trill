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
using System.Reactive.Subjects;
using System.Threading;
using Microsoft.StreamProcessing;

namespace EventHubReceiver
{
    /// <summary>
    /// Comprehensive sample demonstrating strongly-typed C# records and classes with Trill
    /// Shows complex nested types, inheritance, composition, and various type patterns
    /// </summary>
    public static class TypedRecordSample
    {
        public static void Run()
        {
            // Force row-based execution for abstract types and complex nested structures
            // Abstract classes cannot be decomposed into columns
            Config.ForceRowBasedExecution = true;

            Console.WriteLine("╔════════════════════════════════════════════════════════════════════╗");
            Console.WriteLine("║       Strongly-Typed Records & Classes with Trill Sample          ║");
            Console.WriteLine("╚════════════════════════════════════════════════════════════════════╝");
            Console.WriteLine();

            // Display type hierarchy
            DisplayTypeHierarchy();
            Console.WriteLine();

            // Run samples
            RunSimpleRecordExample();
            Console.WriteLine("\n" + new string('═', 72) + "\n");

            RunNestedStructureExample();
            Console.WriteLine("\n" + new string('═', 72) + "\n");

            RunInheritanceExample();
            Console.WriteLine("\n" + new string('═', 72) + "\n");

            RunComplexAggregationExample();
            Console.WriteLine("\n" + new string('═', 72) + "\n");

            RunCheckpointExample();
        }

        #region Example 1: Simple Record Types

        /// <summary>
        /// Example 1: Simple C# records with Trill
        /// </summary>
        private static void RunSimpleRecordExample()
        {
            Console.WriteLine("Example 1: Simple C# Records");
            Console.WriteLine("────────────────────────────");
            Console.WriteLine();

            var checkpointDir = Path.Combine(Path.GetTempPath(), "TrillTyped", "SimpleRecords");
            using var processor = new TypedEventProcessor<SensorReading>(
                "sensor-partition",
                checkpointDir,
                CreateSensorQuery);

            processor.Initialize();

            // Generate sensor readings
            var readings = GenerateSensorReadings(20);
            Console.WriteLine($"Processing {readings.Count} sensor readings...\n");

            foreach (var reading in readings)
            {
                processor.ProcessEvent(reading);
                Thread.Sleep(100);
            }

            processor.Flush();
            Thread.Sleep(2000);

            Console.WriteLine("\n✓ Simple record processing complete");
        }

        #endregion

        #region Example 2: Nested Structures

        /// <summary>
        /// Example 2: Complex nested structures
        /// </summary>
        private static void RunNestedStructureExample()
        {
            Console.WriteLine("Example 2: Complex Nested Structures");
            Console.WriteLine("────────────────────────────────────");
            Console.WriteLine();

            var checkpointDir = Path.Combine(Path.GetTempPath(), "TrillTyped", "NestedStructures");
            using var processor = new TypedEventProcessor<OrderEvent>(
                "order-partition",
                checkpointDir,
                CreateOrderQuery);

            processor.Initialize();

            // Generate order events with nested data
            var orders = GenerateOrderEvents(15);
            Console.WriteLine($"Processing {orders.Count} order events...\n");

            foreach (var order in orders)
            {
                processor.ProcessEvent(order);
                Thread.Sleep(150);
            }

            processor.Flush();
            Thread.Sleep(2000);

            Console.WriteLine("\n✓ Nested structure processing complete");
        }

        #endregion

        #region Example 3: Inheritance Hierarchy

        /// <summary>
        /// Example 3: Type hierarchy with inheritance
        /// </summary>
        private static void RunInheritanceExample()
        {
            Console.WriteLine("Example 3: Inheritance Hierarchy");
            Console.WriteLine("────────────────────────────────");
            Console.WriteLine();

            var checkpointDir = Path.Combine(Path.GetTempPath(), "TrillTyped", "Inheritance");
            using var processor = new TypedEventProcessor<VehicleEvent>(
                "vehicle-partition",
                checkpointDir,
                CreateVehicleQuery);

            processor.Initialize();

            // Generate mixed vehicle events
            var vehicles = GenerateVehicleEvents(25);
            Console.WriteLine($"Processing {vehicles.Count} vehicle events...\n");

            foreach (var vehicle in vehicles)
            {
                processor.ProcessEvent(vehicle);
                Thread.Sleep(80);
            }

            processor.Flush();
            Thread.Sleep(2000);

            Console.WriteLine("\n✓ Inheritance hierarchy processing complete");
        }

        #endregion

        #region Example 4: Complex Aggregations

        /// <summary>
        /// Example 4: Complex aggregations with strongly-typed data
        /// </summary>
        private static void RunComplexAggregationExample()
        {
            Console.WriteLine("Example 4: Complex Aggregations");
            Console.WriteLine("───────────────────────────────");
            Console.WriteLine();

            var checkpointDir = Path.Combine(Path.GetTempPath(), "TrillTyped", "Aggregations");
            using var processor = new TypedEventProcessor<Transaction>(
                "transaction-partition",
                checkpointDir,
                CreateTransactionQuery);

            processor.Initialize();

            // Generate transaction data
            var transactions = GenerateTransactions(30);
            Console.WriteLine($"Processing {transactions.Count} transactions...\n");

            foreach (var transaction in transactions)
            {
                processor.ProcessEvent(transaction);
                Thread.Sleep(50);
            }

            processor.Flush();
            Thread.Sleep(2000);

            Console.WriteLine("\n✓ Complex aggregation processing complete");
        }

        #endregion

        #region Example 5: Checkpoint/Restore

        /// <summary>
        /// Example 5: Checkpoint and restore with strongly-typed data
        /// </summary>
        private static void RunCheckpointExample()
        {
            Console.WriteLine("Example 5: Checkpoint & Restore");
            Console.WriteLine("───────────────────────────────");
            Console.WriteLine();

            var checkpointDir = Path.Combine(Path.GetTempPath(), "TrillTyped", "Checkpoint");

            // Phase 1: Process and checkpoint
            Console.WriteLine("Phase 1: Processing and checkpointing...");
            using (var processor = new TypedEventProcessor<MetricEvent>(
                "metric-partition",
                checkpointDir,
                CreateMetricQuery))
            {
                processor.Initialize();

                var metrics = GenerateMetricEvents(10);
                foreach (var metric in metrics)
                {
                    processor.ProcessEvent(metric);
                    Thread.Sleep(100);
                }

                processor.Flush();
                Thread.Sleep(11000); // Wait for checkpoint
                Console.WriteLine("✓ Phase 1 complete, checkpoint saved");
            }

            Console.WriteLine("\nPhase 2: Restoring and continuing...");
            using (var processor = new TypedEventProcessor<MetricEvent>(
                "metric-partition",
                checkpointDir,
                CreateMetricQuery))
            {
                processor.Initialize(); // Will restore from checkpoint

                var metrics = GenerateMetricEvents(10, startIndex: 10);
                foreach (var metric in metrics)
                {
                    processor.ProcessEvent(metric);
                    Thread.Sleep(100);
                }

                processor.Flush();
                Thread.Sleep(2000);
                Console.WriteLine("✓ Phase 2 complete, state restored successfully");
            }

            Console.WriteLine("\n✓ Checkpoint/restore demonstration complete");
        }

        #endregion

        #region Query Factories

        private static IStreamable<Empty, object> CreateSensorQuery(
            IStreamable<Empty, SensorReading> input)
        {
            // Use TumblingWindow instead of HoppingWindow for row-based execution compatibility
            return input
                .TumblingWindowLifetime(TimeSpan.FromSeconds(5).Ticks)
                .Aggregate(
                    w => w.Count(),
                    w => w.Average(e => e.Temperature),
                    w => w.Max(e => e.Temperature),
                    w => w.Min(e => e.Temperature),
                    (count, avg, max, min) => (object)new SensorStats
                    {
                        ReadingCount = count,
                        AverageTemperature = avg,
                        MaxTemperature = max,
                        MinTemperature = min
                    });
        }

        private static IStreamable<Empty, object> CreateOrderQuery(
            IStreamable<Empty, OrderEvent> input)
        {
            return input
                .TumblingWindowLifetime(TimeSpan.FromSeconds(5).Ticks)
                .Aggregate(
                    w => w.Count(),
                    w => w.Sum(e => e.TotalAmount),
                    w => w.Average(e => e.Items.Count),
                    (count, totalAmount, avgItems) => (object)new OrderStats
                    {
                        OrderCount = count,
                        TotalRevenue = totalAmount,
                        AverageItemsPerOrder = avgItems
                    });
        }

        private static IStreamable<Empty, object> CreateVehicleQuery(
            IStreamable<Empty, VehicleEvent> input)
        {
            return input
                .TumblingWindowLifetime(TimeSpan.FromSeconds(5).Ticks)
                .Aggregate(
                    w => w.Count(),
                    w => w.Average(e => e.Speed),
                    w => w.Max(e => e.Speed),
                    (count, avgSpeed, maxSpeed) => (object)new VehicleStats
                    {
                        VehicleCount = count,
                        AverageSpeed = avgSpeed,
                        MaxSpeed = maxSpeed
                    });
        }

        private static IStreamable<Empty, object> CreateTransactionQuery(
            IStreamable<Empty, Transaction> input)
        {
            return input
                .TumblingWindowLifetime(TimeSpan.FromSeconds(5).Ticks)
                .Aggregate(
                    w => w.Count(),
                    w => w.Sum(e => e.Amount),
                    w => w.Average(e => (double)e.Amount),
                    (count, sum, avg) => (object)new TransactionStats
                    {
                        TransactionCount = count,
                        TotalAmount = sum,
                        AverageAmount = (decimal)avg
                    });
        }

        private static IStreamable<Empty, object> CreateMetricQuery(
            IStreamable<Empty, MetricEvent> input)
        {
            return input
                .TumblingWindowLifetime(TimeSpan.FromSeconds(5).Ticks)
                .Aggregate(
                    w => w.Count(),
                    w => w.Average(e => e.Value),
                    (count, avg) => (object)new MetricStats
                    {
                        Count = count,
                        Average = avg
                    });
        }

        #endregion

        #region Data Generators

        private static List<StreamEvent<SensorReading>> GenerateSensorReadings(int count)
        {
            var events = new List<StreamEvent<SensorReading>>();
            var random = new Random(42);
            var startTime = DateTime.UtcNow;

            for (int i = 0; i < count; i++)
            {
                var reading = new SensorReading
                {
                    SensorId = $"Sensor-{random.Next(1, 6)}",
                    Timestamp = startTime.AddSeconds(i),
                    Temperature = 20.0 + (random.NextDouble() * 15.0),
                    Humidity = 30.0 + (random.NextDouble() * 40.0),
                    Location = new GeoLocation
                    {
                        Latitude = 37.7749 + (random.NextDouble() * 0.1),
                        Longitude = -122.4194 + (random.NextDouble() * 0.1),
                        Altitude = random.Next(0, 100)
                    }
                };

                events.Add(StreamEvent.CreateStart(startTime.AddSeconds(i).Ticks, reading));
            }

            return events;
        }

        private static List<StreamEvent<OrderEvent>> GenerateOrderEvents(int count)
        {
            var events = new List<StreamEvent<OrderEvent>>();
            var random = new Random(42);
            var startTime = DateTime.UtcNow;

            for (int i = 0; i < count; i++)
            {
                var order = new OrderEvent
                {
                    OrderId = Guid.NewGuid(),
                    CustomerId = $"Cust-{random.Next(1, 10)}",
                    OrderTime = startTime.AddSeconds(i * 2),
                    Items = GenerateOrderItems(random.Next(1, 5), random),
                    ShippingAddress = new Address
                    {
                        Street = $"{random.Next(100, 999)} Main St",
                        City = new[] { "Seattle", "Portland", "San Francisco" }[random.Next(3)],
                        State = "WA",
                        ZipCode = $"{random.Next(98000, 98999)}",
                        Country = "USA"
                    },
                    Payment = new PaymentInfo
                    {
                        Method = (PaymentMethod)random.Next(0, 3),
                        LastFourDigits = $"{random.Next(1000, 9999)}",
                        IsVerified = random.Next(2) == 0
                    }
                };

                order.TotalAmount = order.Items.Sum(item => item.Price * item.Quantity);

                events.Add(StreamEvent.CreateStart(startTime.AddSeconds(i * 2).Ticks, order));
            }

            return events;
        }

        private static List<OrderItem> GenerateOrderItems(int count, Random random)
        {
            var items = new List<OrderItem>();
            var products = new[] { "Widget", "Gadget", "Gizmo", "Doohickey", "Thingamajig" };

            for (int i = 0; i < count; i++)
            {
                items.Add(new OrderItem
                {
                    ProductId = $"PROD-{random.Next(1000, 9999)}",
                    ProductName = products[random.Next(products.Length)],
                    Quantity = random.Next(1, 10),
                    Price = (decimal)Math.Round(10.0 + (random.NextDouble() * 90.0), 2)
                });
            }

            return items;
        }

        private static List<StreamEvent<VehicleEvent>> GenerateVehicleEvents(int count)
        {
            var events = new List<StreamEvent<VehicleEvent>>();
            var random = new Random(42);
            var startTime = DateTime.UtcNow;

            for (int i = 0; i < count; i++)
            {
                VehicleEvent vehicle = (i % 3) switch
                {
                    0 => new CarEvent
                    {
                        VehicleId = $"CAR-{random.Next(1000, 9999)}",
                        Timestamp = startTime.AddSeconds(i),
                        Speed = random.Next(30, 80),
                        Position = new GeoLocation
                        {
                            Latitude = 47.6062 + random.NextDouble() * 0.1,
                            Longitude = -122.3321 + random.NextDouble() * 0.1
                        },
                        NumberOfDoors = random.Next(2, 5),
                        HasSunroof = random.Next(2) == 0
                    },
                    1 => new TruckEvent
                    {
                        VehicleId = $"TRK-{random.Next(1000, 9999)}",
                        Timestamp = startTime.AddSeconds(i),
                        Speed = random.Next(40, 70),
                        Position = new GeoLocation
                        {
                            Latitude = 47.6062 + random.NextDouble() * 0.1,
                            Longitude = -122.3321 + random.NextDouble() * 0.1
                        },
                        CargoWeight = random.Next(1000, 5000),
                        NumberOfAxles = random.Next(2, 4)
                    },
                    _ => new MotorcycleEvent
                    {
                        VehicleId = $"BIKE-{random.Next(1000, 9999)}",
                        Timestamp = startTime.AddSeconds(i),
                        Speed = random.Next(50, 120),
                        Position = new GeoLocation
                        {
                            Latitude = 47.6062 + random.NextDouble() * 0.1,
                            Longitude = -122.3321 + random.NextDouble() * 0.1
                        },
                        EngineCC = random.Next(500, 1200),
                        HasSidecar = random.Next(10) == 0
                    }
                };

                events.Add(StreamEvent.CreateStart(startTime.AddSeconds(i).Ticks, vehicle));
            }

            return events;
        }

        private static List<StreamEvent<Transaction>> GenerateTransactions(int count)
        {
            var events = new List<StreamEvent<Transaction>>();
            var random = new Random(42);
            var startTime = DateTime.UtcNow;

            for (int i = 0; i < count; i++)
            {
                var transaction = new Transaction
                {
                    TransactionId = Guid.NewGuid(),
                    AccountId = $"ACC-{random.Next(1000, 1010)}",
                    Timestamp = startTime.AddSeconds(i * 0.5),
                    Amount = (decimal)Math.Round(10.0 + (random.NextDouble() * 990.0), 2),
                    Currency = "USD",
                    Type = (TransactionType)random.Next(0, 3),
                    Merchant = new MerchantInfo
                    {
                        MerchantId = $"MRCH-{random.Next(100, 200)}",
                        Name = $"Store {random.Next(1, 20)}",
                        Category = new[] { "Retail", "Food", "Gas", "Entertainment" }[random.Next(4)],
                        Location = new GeoLocation
                        {
                            Latitude = 37.7749 + random.NextDouble() * 0.5,
                            Longitude = -122.4194 + random.NextDouble() * 0.5
                        }
                    },
                    FraudScore = random.NextDouble()
                };

                events.Add(StreamEvent.CreateStart(startTime.AddSeconds(i * 0.5).Ticks, transaction));
            }

            return events;
        }

        private static List<StreamEvent<MetricEvent>> GenerateMetricEvents(int count, int startIndex = 0)
        {
            var events = new List<StreamEvent<MetricEvent>>();
            var random = new Random(42 + startIndex);
            var startTime = DateTime.UtcNow;

            for (int i = 0; i < count; i++)
            {
                var metric = new MetricEvent
                {
                    MetricId = Guid.NewGuid(),
                    Name = new[] { "CPU", "Memory", "Disk", "Network" }[random.Next(4)],
                    Value = random.NextDouble() * 100,
                    Timestamp = startTime.AddSeconds((startIndex + i) * 0.5),
                    Tags = new Dictionary<string, string>
                    {
                        { "Host", $"Server-{random.Next(1, 5)}" },
                        { "Region", new[] { "US-West", "US-East", "EU" }[random.Next(3)] }
                    }
                };

                events.Add(StreamEvent.CreateStart(startTime.AddSeconds((startIndex + i) * 0.5).Ticks, metric));
            }

            return events;
        }

        #endregion

        #region Type Hierarchy Display

        private static void DisplayTypeHierarchy()
        {
            Console.WriteLine(@"
Type Hierarchy (UML-like representation):
═════════════════════════════════════════

1. SIMPLE RECORD TYPE
   ┌────────────────────────────┐
   │   record SensorReading     │
   ├────────────────────────────┤
   │ + SensorId: string         │
   │ + Timestamp: DateTime      │
   │ + Temperature: double      │
   │ + Humidity: double         │
   │ + Location: GeoLocation    │◄───┐
   └────────────────────────────┘    │
                                      │
   ┌──────────────────────────────┐  │
   │   record GeoLocation         │◄─┘
   ├──────────────────────────────┤
   │ + Latitude: double           │
   │ + Longitude: double          │
   │ + Altitude: int?             │
   └──────────────────────────────┘

2. NESTED STRUCTURE
   ┌────────────────────────────┐
   │   record OrderEvent        │
   ├────────────────────────────┤
   │ + OrderId: Guid            │
   │ + CustomerId: string       │
   │ + OrderTime: DateTime      │
   │ + Items: List<OrderItem>   │◄───┐
   │ + ShippingAddress: Address │◄───┤─┐
   │ + Payment: PaymentInfo     │◄───┤─┼┐
   │ + TotalAmount: decimal     │    │ ││
   └────────────────────────────┘    │ ││
                                      │ ││
   ┌──────────────────────────┐      │ ││
   │   record OrderItem       │◄─────┘ ││
   ├──────────────────────────┤        ││
   │ + ProductId: string      │        ││
   │ + ProductName: string    │        ││
   │ + Quantity: int          │        ││
   │ + Price: decimal         │        ││
   └──────────────────────────┘        ││
                                        ││
   ┌──────────────────────────┐        ││
   │   record Address         │◄───────┘│
   ├──────────────────────────┤         │
   │ + Street: string         │         │
   │ + City: string           │         │
   │ + State: string          │         │
   │ + ZipCode: string        │         │
   │ + Country: string        │         │
   └──────────────────────────┘         │
                                         │
   ┌──────────────────────────┐         │
   │   record PaymentInfo     │◄────────┘
   ├──────────────────────────┤
   │ + Method: PaymentMethod  │
   │ + LastFourDigits: string │
   │ + IsVerified: bool       │
   └──────────────────────────┘

3. INHERITANCE HIERARCHY
              ┌───────────────────────┐
              │  abstract class       │
              │   VehicleEvent        │
              ├───────────────────────┤
              │ + VehicleId: string   │
              │ + Timestamp: DateTime │
              │ + Speed: double       │
              │ + Position: GeoLoc    │
              └───────────────────────┘
                       ▲
         ┌─────────────┼─────────────┐
         │             │             │
   ┌─────────┐  ┌──────────┐  ┌─────────────┐
   │ CarEvent│  │TruckEvent│  │ Motorcycle  │
   │         │  │          │  │   Event     │
   ├─────────┤  ├──────────┤  ├─────────────┤
   │ +Doors  │  │ +Cargo   │  │ +EngineCC   │
   │ +Sunroof│  │ +Axles   │  │ +HasSidecar │
   └─────────┘  └──────────┘  └─────────────┘

4. COMPLEX COMPOSITION
   ┌────────────────────────────┐
   │   record Transaction       │
   ├────────────────────────────┤
   │ + TransactionId: Guid      │
   │ + AccountId: string        │
   │ + Amount: decimal          │
   │ + Type: TransactionType    │◄─── enum
   │ + Merchant: MerchantInfo   │◄───┐
   │ + FraudScore: double       │    │
   └────────────────────────────┘    │
                                      │
   ┌──────────────────────────────┐  │
   │   record MerchantInfo        │◄─┘
   ├──────────────────────────────┤
   │ + MerchantId: string         │
   │ + Name: string               │
   │ + Category: string           │
   │ + Location: GeoLocation      │
   └──────────────────────────────┘
");
        }

        #endregion
    }
}
