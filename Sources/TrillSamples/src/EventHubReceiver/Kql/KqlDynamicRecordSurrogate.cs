// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using Microsoft.StreamProcessing.Serializer;

namespace EventHubReceiver.Kql
{
    /// <summary>
    /// Surrogate serializer for KqlDynamicRecord that handles schema and data dictionary serialization.
    /// This enables KqlDynamicRecord to be used with Trill's checkpoint/restore functionality.
    /// </summary>
    public sealed class KqlDynamicRecordSurrogate : ISurrogate
    {
        private readonly ObjectDictionarySurrogate dictionarySurrogate = new ObjectDictionarySurrogate();

        /// <summary>
        /// Returns whether the type is KqlDynamicRecord which this surrogate can handle.
        /// </summary>
        public bool IsSupportedType(Type type, out MethodInfo serialize, out MethodInfo deserialize)
        {
            if (type == typeof(KqlDynamicRecord))
            {
                serialize = typeof(KqlDynamicRecordSurrogate).GetMethod(nameof(Serialize));
                deserialize = typeof(KqlDynamicRecordSurrogate).GetMethod(nameof(Deserialize));
                return true;
            }

            // Also delegate to dictionary surrogate for Dictionary<string, object>
            if (type.IsGenericType &&
                type.GetGenericTypeDefinition() == typeof(Dictionary<,>) &&
                type.GetGenericArguments()[0] == typeof(string) &&
                type.GetGenericArguments()[1] == typeof(object))
            {
                return dictionarySurrogate.IsSupportedType(type, out serialize, out deserialize);
            }

            serialize = null;
            deserialize = null;
            return false;
        }

        /// <summary>
        /// Serializes a KqlDynamicRecord by writing the schema and data.
        /// </summary>
        public void Serialize(KqlDynamicRecord record, Stream stream)
        {
            using (var writer = new BinaryWriter(stream, System.Text.Encoding.UTF8, leaveOpen: true))
            {
                if (record == null)
                {
                    writer.Write((byte)0); // null marker
                    return;
                }

                writer.Write((byte)1); // not null marker

                // Write event counter
                writer.Write(record.EventCounter);

                // Write schema table name
                writer.Write(record.Schema?.TableName ?? string.Empty);

                // Write data dictionary using ObjectDictionarySurrogate
                var data = record.GetData();
                dictionarySurrogate.Serialize(data, stream);
            }
        }

        /// <summary>
        /// Deserializes a KqlDynamicRecord.
        /// Note: Schema reconstruction is limited - the record will be reconstructed with minimal schema info.
        /// For full functionality, the schema should be provided separately.
        /// </summary>
        public KqlDynamicRecord Deserialize(Stream stream)
        {
            using (var reader = new BinaryReader(stream, System.Text.Encoding.UTF8, leaveOpen: true))
            {
                var isNotNull = reader.ReadByte();

                if (isNotNull == 0)
                {
                    return null;
                }

                // Read event counter
                var eventCounter = reader.ReadInt64();

                // Read schema table name
                var tableName = reader.ReadString();

                // Read data dictionary
                var data = dictionarySurrogate.Deserialize(stream);

                // Create a minimal schema from the data
                var schema = CreateMinimalSchema(tableName, data);

                // Create record
                var record = KqlDynamicRecord.FromDictionary(data, schema);
                record.EventCounter = eventCounter;

                return record;
            }
        }

        /// <summary>
        /// Creates a minimal schema from dictionary data.
        /// This infers types from the actual values.
        /// </summary>
        private static KqlTableSchema CreateMinimalSchema(string tableName, Dictionary<string, object> data)
        {
            var schema = new KqlTableSchema { TableName = tableName };

            foreach (var kvp in data)
            {
                var dataType = InferKqlDataType(kvp.Value);
                var clrType = KqlSchemaParser.GetClrType(dataType);

                schema.Columns.Add(new KqlColumn
                {
                    Name = kvp.Key,
                    DataType = dataType,
                    ClrType = clrType,
                    IsNullable = true
                });
            }

            return schema;
        }

        /// <summary>
        /// Infers KQL data type from a value
        /// </summary>
        private static KqlDataType InferKqlDataType(object value)
        {
            if (value == null)
                return KqlDataType.Dynamic;

            return value switch
            {
                bool => KqlDataType.Bool,
                DateTime => KqlDataType.DateTime,
                Guid => KqlDataType.Guid,
                int => KqlDataType.Int,
                long => KqlDataType.Long,
                double or float => KqlDataType.Real,
                decimal => KqlDataType.Decimal,
                string => KqlDataType.String,
                TimeSpan => KqlDataType.Timespan,
                Dictionary<string, object> => KqlDataType.Dynamic,
                _ => KqlDataType.Dynamic
            };
        }
    }

    /// <summary>
    /// Combined surrogate that handles both KqlDynamicRecord and Dictionary&lt;string, object&gt;.
    /// Use this when creating QueryContainer for KQL-based event processing.
    /// </summary>
    public sealed class KqlSurrogate : ISurrogate
    {
        private readonly KqlDynamicRecordSurrogate recordSurrogate = new KqlDynamicRecordSurrogate();
        private readonly ObjectDictionarySurrogate dictionarySurrogate = new ObjectDictionarySurrogate();

        /// <summary>
        /// Returns whether the type is supported by either surrogate.
        /// </summary>
        public bool IsSupportedType(Type type, out MethodInfo serialize, out MethodInfo deserialize)
        {
            // Check KqlDynamicRecord first
            if (type == typeof(KqlDynamicRecord))
            {
                // Return OUR methods, not the delegate's methods
                serialize = typeof(KqlSurrogate).GetMethod(nameof(SerializeRecord));
                deserialize = typeof(KqlSurrogate).GetMethod(nameof(DeserializeRecord));
                return true;
            }

            // Check Dictionary<string, object>
            if (type.IsGenericType &&
                type.GetGenericTypeDefinition() == typeof(Dictionary<,>) &&
                type.GetGenericArguments()[0] == typeof(string) &&
                type.GetGenericArguments()[1] == typeof(object))
            {
                // Return OUR methods, not the delegate's methods
                serialize = typeof(KqlSurrogate).GetMethod(nameof(SerializeDictionary));
                deserialize = typeof(KqlSurrogate).GetMethod(nameof(DeserializeDictionary));
                return true;
            }

            serialize = null;
            deserialize = null;
            return false;
        }

        /// <summary>
        /// Serialize KqlDynamicRecord - delegates to recordSurrogate
        /// </summary>
        public void SerializeRecord(KqlDynamicRecord record, Stream stream)
        {
            recordSurrogate.Serialize(record, stream);
        }

        /// <summary>
        /// Deserialize KqlDynamicRecord - delegates to recordSurrogate
        /// </summary>
        public KqlDynamicRecord DeserializeRecord(Stream stream)
        {
            return recordSurrogate.Deserialize(stream);
        }

        /// <summary>
        /// Serialize Dictionary - delegates to dictionarySurrogate
        /// </summary>
        public void SerializeDictionary(Dictionary<string, object> dict, Stream stream)
        {
            dictionarySurrogate.Serialize(dict, stream);
        }

        /// <summary>
        /// Deserialize Dictionary - delegates to dictionarySurrogate
        /// </summary>
        public Dictionary<string, object> DeserializeDictionary(Stream stream)
        {
            return dictionarySurrogate.Deserialize(stream);
        }
    }
}
