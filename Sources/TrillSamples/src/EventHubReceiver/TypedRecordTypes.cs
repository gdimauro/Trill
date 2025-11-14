// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Runtime.Serialization;

namespace EventHubReceiver
{
    // ?????????????????????????????????????????????????????????????????????
    // SIMPLE RECORD TYPES - Basic strongly-typed events
    // ?????????????????????????????????????????????????????????????????????

    /// <summary>
    /// Sensor reading with embedded location data
    /// Demonstrates: Simple record type with nested struct
    /// </summary>
    public record SensorReading
    {
        public string SensorId { get; init; }
        public DateTime Timestamp { get; init; }
        public double Temperature { get; init; }
        public double Humidity { get; init; }
        public GeoLocation Location { get; init; }
    }

    /// <summary>
    /// Geographic location struct
    /// Demonstrates: Reusable value type for composition
    /// </summary>
    public record struct GeoLocation
    {
        public double Latitude { get; init; }
        public double Longitude { get; init; }
        public int? Altitude { get; init; }
    }

    /// <summary>
    /// Statistics output for sensor data
    /// </summary>
    public record SensorStats
    {
        public ulong ReadingCount { get; init; }
        public double AverageTemperature { get; init; }
        public double MaxTemperature { get; init; }
        public double MinTemperature { get; init; }
    }

    // ?????????????????????????????????????????????????????????????????????
    // NESTED STRUCTURES - Complex composition patterns
    // ?????????????????????????????????????????????????????????????????????

    /// <summary>
    /// Order event with multiple nested structures
    /// Demonstrates: Deep composition with Lists, enums, and nested records
    /// </summary>
    public record OrderEvent
    {
        public Guid OrderId { get; init; }
        public string CustomerId { get; init; }
        public DateTime OrderTime { get; init; }
        public List<OrderItem> Items { get; init; } = new();
        public Address ShippingAddress { get; init; }
        public PaymentInfo Payment { get; init; }
        public decimal TotalAmount { get; set; }
    }

    /// <summary>
    /// Individual order item
    /// Demonstrates: Collection element type
    /// </summary>
    public record OrderItem
    {
        public string ProductId { get; init; }
        public string ProductName { get; init; }
        public int Quantity { get; init; }
        public decimal Price { get; init; }
    }

    /// <summary>
    /// Shipping address information
    /// Demonstrates: Structured address data
    /// </summary>
    public record Address
    {
        public string Street { get; init; }
        public string City { get; init; }
        public string State { get; init; }
        public string ZipCode { get; init; }
        public string Country { get; init; }
    }

    /// <summary>
    /// Payment information
    /// Demonstrates: Sensitive data with enum
    /// </summary>
    public record PaymentInfo
    {
        public PaymentMethod Method { get; init; }
        public string LastFourDigits { get; init; }
        public bool IsVerified { get; init; }
    }

    /// <summary>
    /// Payment method enumeration
    /// </summary>
    public enum PaymentMethod
    {
        CreditCard,
        DebitCard,
        PayPal
    }

    /// <summary>
    /// Order statistics output
    /// </summary>
    public record OrderStats
    {
        public ulong OrderCount { get; init; }
        public decimal TotalRevenue { get; init; }
        public double AverageItemsPerOrder { get; init; }
    }

    // ?????????????????????????????????????????????????????????????????????
    // INHERITANCE HIERARCHY - Polymorphic type system
    // ?????????????????????????????????????????????????????????????????????

    /// <summary>
    /// Base vehicle event (abstract base class)
    /// Demonstrates: Inheritance hierarchy with Trill
    /// </summary>
    [KnownType(typeof(CarEvent))]
    [KnownType(typeof(TruckEvent))]
    [KnownType(typeof(MotorcycleEvent))]
    public abstract class VehicleEvent
    {
        public string VehicleId { get; set; }
        public DateTime Timestamp { get; set; }
        public double Speed { get; set; }
        public GeoLocation Position { get; set; }
    }

    /// <summary>
    /// Car-specific event
    /// Demonstrates: Derived type with additional properties
    /// </summary>
    public class CarEvent : VehicleEvent
    {
        public int NumberOfDoors { get; set; }
        public bool HasSunroof { get; set; }
    }

    /// <summary>
    /// Truck-specific event
    /// Demonstrates: Another derived type
    /// </summary>
    public class TruckEvent : VehicleEvent
    {
        public int CargoWeight { get; set; }
        public int NumberOfAxles { get; set; }
    }

    /// <summary>
    /// Motorcycle-specific event
    /// Demonstrates: Third variant in hierarchy
    /// </summary>
    public class MotorcycleEvent : VehicleEvent
    {
        public int EngineCC { get; set; }
        public bool HasSidecar { get; set; }
    }

    /// <summary>
    /// Vehicle statistics output
    /// </summary>
    public record VehicleStats
    {
        public ulong VehicleCount { get; init; }
        public double AverageSpeed { get; init; }
        public double MaxSpeed { get; init; }
    }

    // ?????????????????????????????????????????????????????????????????????
    // COMPLEX COMPOSITION - Advanced patterns
    // ?????????????????????????????????????????????????????????????????????

    /// <summary>
    /// Financial transaction with nested merchant data
    /// Demonstrates: Complex business entity with multiple nested types
    /// </summary>
    public record Transaction
    {
        public Guid TransactionId { get; init; }
        public string AccountId { get; init; }
        public DateTime Timestamp { get; init; }
        public decimal Amount { get; init; }
        public string Currency { get; init; }
        public TransactionType Type { get; init; }
        public MerchantInfo Merchant { get; init; }
        public double FraudScore { get; init; }
    }

    /// <summary>
    /// Transaction type enumeration
    /// </summary>
    public enum TransactionType
    {
        Purchase,
        Refund,
        Transfer
    }

    /// <summary>
    /// Merchant information
    /// Demonstrates: Nested business entity
    /// </summary>
    public record MerchantInfo
    {
        public string MerchantId { get; init; }
        public string Name { get; init; }
        public string Category { get; init; }
        public GeoLocation Location { get; init; }
    }

    /// <summary>
    /// Transaction statistics output
    /// </summary>
    public record TransactionStats
    {
        public ulong TransactionCount { get; init; }
        public decimal TotalAmount { get; init; }
        public decimal AverageAmount { get; init; }
    }

    // ?????????????????????????????????????????????????????????????????????
    // METRIC TYPES - Checkpointing demonstration
    // ?????????????????????????????????????????????????????????????????????

    /// <summary>
    /// Metric event with tags dictionary
    /// Demonstrates: Record with Dictionary property
    /// </summary>
    public record MetricEvent
    {
        public Guid MetricId { get; init; }
        public string Name { get; init; }
        public double Value { get; init; }
        public DateTime Timestamp { get; init; }
        public Dictionary<string, string> Tags { get; init; } = new();
    }

    /// <summary>
    /// Metric statistics output
    /// </summary>
    public record MetricStats
    {
        public ulong Count { get; init; }
        public double Average { get; init; }
        public double Minimum { get; init; }
        public double Maximum { get; init; }
        public DateTime Timestamp { get; init; }
    }
}
