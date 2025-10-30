// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text.Json;

namespace Microsoft.StreamProcessing.Serializer
{
    /// <summary>
    /// Surrogate serializer for Dictionary&lt;string, object&gt; that converts values to/from JSON strings.
    /// This allows serialization of dictionaries with object values without requiring KnownType attributes.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This surrogate automatically handles Dictionary&lt;string, object&gt; by serializing the object values as JSON strings.
    /// Upon deserialization, the JSON is parsed back into basic CLR types (int, long, double, string, bool, List&lt;object&gt;, Dictionary&lt;string, object&gt;).
    /// </para>
    /// <para>
    /// <strong>Usage:</strong>
    /// <code>
    /// var qc = new QueryContainer(new ObjectDictionarySurrogate());
    /// // Now you can serialize types containing Dictionary&lt;string, object&gt;
    /// </code>
    /// </para>
    /// <para>
    /// <strong>Limitations:</strong>
    /// - Object values must be JSON-serializable
    /// - Complex types will be deserialized as Dictionary&lt;string, object&gt; or List&lt;object&gt;, not as their original types
    /// - For better type fidelity, consider using specific types or KnownType attributes instead
    /// </para>
    /// </remarks>
    public sealed class ObjectDictionarySurrogate : ISurrogate
    {
        private static readonly JsonSerializerOptions JsonOptions = new JsonSerializerOptions
        {
            WriteIndented = false,
            DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.Never
        };

        /// <summary>
        /// Returns whether the type is Dictionary&lt;string, object&gt; which this surrogate can handle.
        /// </summary>
        public bool IsSupportedType(Type type, out MethodInfo serialize, out MethodInfo deserialize)
        {
            // Check if this is Dictionary<string, object>
            if (type.IsGenericType && 
                type.GetGenericTypeDefinition() == typeof(Dictionary<,>) &&
                type.GetGenericArguments()[0] == typeof(string) &&
                type.GetGenericArguments()[1] == typeof(object))
            {
                serialize = typeof(ObjectDictionarySurrogate).GetMethod(nameof(Serialize));
                deserialize = typeof(ObjectDictionarySurrogate).GetMethod(nameof(Deserialize));
                return true;
            }

            serialize = null;
            deserialize = null;
            return false;
        }

        /// <summary>
        /// Serializes a Dictionary&lt;string, object&gt; by converting object values to JSON strings.
        /// </summary>
        public void Serialize(Dictionary<string, object> dict, Stream stream)
        {
            using (var writer = new BinaryWriter(stream, System.Text.Encoding.UTF8, leaveOpen: true))
            {
                if (dict == null)
                {
                    writer.Write(-1); // null marker
                    return;
                }

                writer.Write(dict.Count);
                foreach (var kvp in dict)
                {
                    // Serialize key
                    writer.Write(kvp.Key ?? string.Empty);
                    
                    // Serialize value as JSON string
                    if (kvp.Value == null)
                    {
                        writer.Write((byte)0); // null marker
                    }
                    else
                    {
                        writer.Write((byte)1); // not null marker
                        try
                        {
                            var jsonValue = JsonSerializer.Serialize(kvp.Value, JsonOptions);
                            writer.Write(jsonValue);
                        }
                        catch (Exception ex)
                        {
                            throw new System.Runtime.Serialization.SerializationException(
                                $"Failed to serialize value for key '{kvp.Key}' of type '{kvp.Value.GetType()}'. " +
                                $"The value must be JSON-serializable. Error: {ex.Message}", ex);
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Deserializes a Dictionary&lt;string, object&gt; by converting JSON strings back to objects.
        /// </summary>
        public Dictionary<string, object> Deserialize(Stream stream)
        {
            using (var reader = new BinaryReader(stream, System.Text.Encoding.UTF8, leaveOpen: true))
            {
                var count = reader.ReadInt32();
                
                if (count == -1)
                {
                    return null; // null dictionary
                }

                var dict = new Dictionary<string, object>(count);
                
                for (int i = 0; i < count; i++)
                {
                    var key = reader.ReadString();
                    var isNotNull = reader.ReadByte();
                    
                    if (isNotNull == 0)
                    {
                        dict[key] = null;
                    }
                    else
                    {
                        var jsonValue = reader.ReadString();
                        try
                        {
                            // Deserialize as JsonElement to preserve the original structure
                            var element = JsonSerializer.Deserialize<JsonElement>(jsonValue);
                            dict[key] = ConvertJsonElement(element);
                        }
                        catch (Exception ex)
                        {
                            throw new System.Runtime.Serialization.SerializationException(
                                $"Failed to deserialize value for key '{key}'. Error: {ex.Message}", ex);
                        }
                    }
                }
                
                return dict;
            }
        }

        private static object ConvertJsonElement(JsonElement element)
        {
            switch (element.ValueKind)
            {
                case JsonValueKind.Null:
                    return null;
                case JsonValueKind.True:
                    return true;
                case JsonValueKind.False:
                    return false;
                case JsonValueKind.Number:
                    if (element.TryGetInt32(out int intValue))
                        return intValue;
                    if (element.TryGetInt64(out long longValue))
                        return longValue;
                    if (element.TryGetDouble(out double doubleValue))
                        return doubleValue;
                    return element.GetDecimal();
                case JsonValueKind.String:
                    return element.GetString();
                case JsonValueKind.Array:
                    var list = new List<object>();
                    foreach (var item in element.EnumerateArray())
                    {
                        list.Add(ConvertJsonElement(item));
                    }
                    return list;
                case JsonValueKind.Object:
                    var dict = new Dictionary<string, object>();
                    foreach (var property in element.EnumerateObject())
                    {
                        dict[property.Name] = ConvertJsonElement(property.Value);
                    }
                    return dict;
                default:
                    return element.ToString();
            }
        }
    }
}
