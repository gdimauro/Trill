// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq;

namespace EventHubReceiver.Kql
{
    /// <summary>
    /// Dynamic record that conforms to a KQL table schema
    /// Provides strongly-typed access to fields while maintaining flexibility
    /// Compatible with Trill serialization via Dictionary backing store
    /// </summary>
    public sealed class KqlDynamicRecord
    {
        private readonly Dictionary<string, object> data = new Dictionary<string, object>();
        private readonly KqlTableSchema schema;

        /// <summary>
        /// Schema that defines this record's structure
        /// </summary>
        public KqlTableSchema Schema => schema;

        /// <summary>
        /// Event counter for tracking across checkpoint/restore cycles
        /// </summary>
        public long EventCounter { get; set; }

        /// <summary>
        /// Create a new KQL dynamic record with a schema
        /// </summary>
        public KqlDynamicRecord(KqlTableSchema schema)
        {
            this.schema = schema ?? throw new ArgumentNullException(nameof(schema));
        }

        /// <summary>
        /// Create from dictionary with schema validation
        /// </summary>
        public static KqlDynamicRecord FromDictionary(Dictionary<string, object> data, KqlTableSchema schema)
        {
            var record = new KqlDynamicRecord(schema);

            foreach (var kvp in data)
            {
                if (schema.HasColumn(kvp.Key))
                {
                    record.SetValue(kvp.Key, kvp.Value);
                }
            }

            return record;
        }

        /// <summary>
        /// Set a value with schema validation
        /// </summary>
        public void SetValue(string columnName, object value)
        {
            var column = schema.GetColumn(columnName);
            if (column == null)
            {
                throw new ArgumentException($"Column '{columnName}' not found in schema '{schema.TableName}'");
            }

            // Validate and convert type if needed
            var convertedValue = ConvertValue(value, column);
            data[columnName] = convertedValue;
        }

        /// <summary>
        /// Get a strongly-typed value
        /// </summary>
        public T GetValue<T>(string columnName, T defaultValue = default)
        {
            var column = schema.GetColumn(columnName);
            if (column == null)
            {
                throw new ArgumentException($"Column '{columnName}' not found in schema '{schema.TableName}'");
            }

            if (!data.TryGetValue(columnName, out var value))
            {
                return defaultValue;
            }

            if (value == null)
            {
                return defaultValue;
            }

            if (value is T typedValue)
            {
                return typedValue;
            }

            // Handle type conversions
            try
            {
                if (typeof(T).IsPrimitive || typeof(T) == typeof(decimal))
                {
                    return (T)Convert.ChangeType(value, typeof(T));
                }

                if (typeof(T) == typeof(string))
                {
                    return (T)(object)value.ToString();
                }

                if (typeof(T) == typeof(DateTime) && value is string dateStr)
                {
                    return (T)(object)DateTime.Parse(dateStr);
                }

                if (typeof(T) == typeof(Guid) && value is string guidStr)
                {
                    return (T)(object)Guid.Parse(guidStr);
                }

                if (typeof(T) == typeof(TimeSpan) && value is string tsStr)
                {
                    return (T)(object)TimeSpan.Parse(tsStr);
                }

                return defaultValue;
            }
            catch
            {
                return defaultValue;
            }
        }

        /// <summary>
        /// Check if a column has a value (not null)
        /// </summary>
        public bool HasValue(string columnName)
        {
            return data.TryGetValue(columnName, out var value) && value != null;
        }

        /// <summary>
        /// Get the underlying data dictionary
        /// </summary>
        public Dictionary<string, object> GetData() => new Dictionary<string, object>(data);

        /// <summary>
        /// Get all column values as key-value pairs
        /// </summary>
        public IEnumerable<KeyValuePair<string, object>> GetAllValues() => data;

        /// <summary>
        /// Convert a value to the expected CLR type based on schema
        /// </summary>
        private object ConvertValue(object value, KqlColumn column)
        {
            if (value == null)
            {
                return null;
            }

            var targetType = column.ClrType;

            // If already correct type, return as-is
            if (targetType.IsInstanceOfType(value))
            {
                return value;
            }

            try
            {
                // Handle special conversions based on KQL data type
                switch (column.DataType)
                {
                    case KqlDataType.Bool:
                        return Convert.ToBoolean(value);

                    case KqlDataType.DateTime:
                        if (value is string dateStr)
                            return DateTime.Parse(dateStr);
                        if (value is long dateTicks)
                            return new DateTime(dateTicks);
                        return Convert.ToDateTime(value);

                    case KqlDataType.Guid:
                        if (value is string guidStr)
                            return Guid.Parse(guidStr);
                        return value;

                    case KqlDataType.Int:
                        return Convert.ToInt32(value);

                    case KqlDataType.Long:
                        return Convert.ToInt64(value);

                    case KqlDataType.Real:
                        return Convert.ToDouble(value);

                    case KqlDataType.String:
                        return value.ToString();

                    case KqlDataType.Timespan:
                        if (value is string tsStr)
                            return TimeSpan.Parse(tsStr);
                        if (value is long tsTicks)
                            return new TimeSpan(tsTicks);
                        return value;

                    case KqlDataType.Decimal:
                        return Convert.ToDecimal(value);

                    case KqlDataType.Dynamic:
                        if (value is Dictionary<string, object> dict)
                            return dict;
                        return value;

                    default:
                        return value;
                }
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    $"Cannot convert value '{value}' to type {column.DataType} for column '{column.Name}'", ex);
            }
        }

        /// <summary>
        /// Create a formatted string representation
        /// </summary>
        public override string ToString()
        {
            var values = schema.Columns
                .Select(col => $"{col.Name}={(data.TryGetValue(col.Name, out var val) ? val?.ToString() ?? "null" : "unset")}")
                .ToList();

            return $"{schema.TableName}({string.Join(", ", values)})";
        }

        /// <summary>
        /// Validate that all required columns have values
        /// </summary>
        public bool IsValid()
        {
            foreach (var column in schema.Columns)
            {
                if (!column.IsNullable && !HasValue(column.Name))
                {
                    return false;
                }
            }

            return true;
        }

        /// <summary>
        /// Get missing required columns
        /// </summary>
        public List<string> GetMissingColumns()
        {
            return schema.Columns
                .Where(col => !col.IsNullable && !HasValue(col.Name))
                .Select(col => col.Name)
                .ToList();
        }

        /// <summary>
        /// Clone this record
        /// </summary>
        public KqlDynamicRecord Clone()
        {
            var clone = new KqlDynamicRecord(schema)
            {
                EventCounter = EventCounter
            };

            foreach (var kvp in data)
            {
                clone.data[kvp.Key] = kvp.Value;
            }

            return clone;
        }
    }

    /// <summary>
    /// Builder for creating KQL dynamic records with fluent syntax
    /// </summary>
    public sealed class KqlDynamicRecordBuilder
    {
        private readonly KqlDynamicRecord record;

        public KqlDynamicRecordBuilder(KqlTableSchema schema)
        {
            record = new KqlDynamicRecord(schema);
        }

        public KqlDynamicRecordBuilder Set(string columnName, object value)
        {
            record.SetValue(columnName, value);
            return this;
        }

        public KqlDynamicRecordBuilder SetEventCounter(long counter)
        {
            record.EventCounter = counter;
            return this;
        }

        public KqlDynamicRecord Build() => record;
    }
}
