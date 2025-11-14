// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace EventHubReceiver.Kql
{
  /// <summary>
  /// Parser for KQL table schema definitions
  /// Supports standard KQL syntax for table definitions
  /// </summary>
  public static class KqlSchemaParser
  {
    private static readonly Dictionary<string, KqlDataType> TypeMapping = new Dictionary<string, KqlDataType>(StringComparer.OrdinalIgnoreCase)
        {
            { "bool", KqlDataType.Bool },
            { "boolean", KqlDataType.Bool },
            { "datetime", KqlDataType.DateTime },
            { "date", KqlDataType.DateTime },
            { "dynamic", KqlDataType.Dynamic },
            { "guid", KqlDataType.Guid },
            { "uniqueid", KqlDataType.Guid },
            { "uuid", KqlDataType.Guid },
            { "int", KqlDataType.Int },
            { "long", KqlDataType.Long },
            { "real", KqlDataType.Real },
            { "double", KqlDataType.Real },
            { "string", KqlDataType.String },
            { "timespan", KqlDataType.Timespan },
            { "time", KqlDataType.Timespan },
            { "decimal", KqlDataType.Decimal }
        };

    private static readonly Dictionary<KqlDataType, Type> ClrTypeMapping = new Dictionary<KqlDataType, Type>
        {
            { KqlDataType.Bool, typeof(bool) },
            { KqlDataType.DateTime, typeof(DateTime) },
            { KqlDataType.Dynamic, typeof(Dictionary<string, object>) },
            { KqlDataType.Guid, typeof(Guid) },
            { KqlDataType.Int, typeof(int) },
            { KqlDataType.Long, typeof(long) },
            { KqlDataType.Real, typeof(double) },
            { KqlDataType.String, typeof(string) },
            { KqlDataType.Timespan, typeof(TimeSpan) },
            { KqlDataType.Decimal, typeof(decimal) }
        };

    /// <summary>
    /// Parse a KQL table schema definition
    /// Supports formats:
    /// - .create table MyTable (Column1: type, Column2: type)
    /// - let schema = datatable(Column1: type, Column2: type)
    /// - Simple: Column1: type, Column2: type
    /// </summary>
    public static KqlTableSchema Parse(string kqlSchema)
    {
      if (string.IsNullOrWhiteSpace(kqlSchema))
      {
        throw new ArgumentException("KQL schema cannot be empty", nameof(kqlSchema));
      }

      var schema = new KqlTableSchema();
      var cleanSchema = kqlSchema.Trim();

      // Extract table name if present
      var tableNameMatch = Regex.Match(cleanSchema, @"(?:\.create\s+table|let)\s+([A-Za-z_][A-Za-z0-9_]*)", RegexOptions.IgnoreCase);
      if (tableNameMatch.Success)
      {
        schema.TableName = tableNameMatch.Groups[1].Value;
      }
      else
      {
        schema.TableName = "DynamicTable";
      }

      // Extract column definitions between parentheses or entire string
      string columnDefs;
      var parenMatch = Regex.Match(cleanSchema, @"\((.*?)\)", RegexOptions.Singleline);
      if (parenMatch.Success)
      {
        columnDefs = parenMatch.Groups[1].Value;
      }
      else
      {
        columnDefs = cleanSchema;
      }

      // Parse individual column definitions
      // Format: ColumnName: Type [, ColumnName: Type]*
      var columns = SplitColumns(columnDefs);

      foreach (var columnDef in columns)
      {
        var column = ParseColumn(columnDef.Trim());
        if (column != null)
        {
          schema.Columns.Add(column);
        }
      }

      if (schema.Columns.Count == 0)
      {
        throw new ArgumentException("No valid columns found in KQL schema", nameof(kqlSchema));
      }

      return schema;
    }

    /// <summary>
    /// Create a sample KQL schema for testing
    /// </summary>
    public static string CreateSampleSchema(string tableName = "EventData")
    {
      return $@".create table {tableName} (
    Timestamp: datetime,
    EventId: guid,
    UserId: string,
    EventType: string,
    Value: long,
    Score: real,
    IsActive: bool,
    Duration: timespan,
    Metadata: dynamic
)";
    }

    /// <summary>
    /// Get CLR type for a KQL data type
    /// </summary>
    public static Type GetClrType(KqlDataType kqlType) => ClrTypeMapping[kqlType];

    /// <summary>
    /// Get KQL data type from string
    /// </summary>
    public static KqlDataType GetKqlDataType(string typeName)
    {
      if (TypeMapping.TryGetValue(typeName, out var kqlType))
      {
        return kqlType;
      }

      throw new ArgumentException($"Unknown KQL type: {typeName}");
    }

    /// <summary>
    /// Validate a value against a KQL data type
    /// </summary>
    public static bool ValidateValue(object value, KqlDataType dataType)
    {
      if (value == null)
      {
        return true; // Nulls are generally allowed
      }

      var clrType = ClrTypeMapping[dataType];
      return clrType.IsInstanceOfType(value);
    }

    /// <summary>
    /// Parse a single column definition
    /// Format: ColumnName: Type
    /// </summary>
    private static KqlColumn ParseColumn(string columnDef)
    {
      if (string.IsNullOrWhiteSpace(columnDef))
      {
        return null;
      }

      // Split on colon
      var parts = columnDef.Split(':');
      if (parts.Length != 2)
      {
        throw new ArgumentException($"Invalid column definition: {columnDef}. Expected format: ColumnName: Type");
      }

      var columnName = parts[0].Trim();
      var typeName = parts[1].Trim();

      if (string.IsNullOrEmpty(columnName))
      {
        throw new ArgumentException($"Column name cannot be empty in: {columnDef}");
      }

      if (!TypeMapping.TryGetValue(typeName, out var kqlType))
      {
        throw new ArgumentException($"Unknown KQL type: {typeName} in column: {columnName}");
      }

      var clrType = ClrTypeMapping[kqlType];

      return new KqlColumn
      {
        Name = columnName,
        DataType = kqlType,
        ClrType = clrType,
        IsNullable = !clrType.IsValueType || Nullable.GetUnderlyingType(clrType) != null
      };
    }

    /// <summary>
    /// Split column definitions, respecting nested structures
    /// </summary>
    private static List<string> SplitColumns(string columnDefs)
    {
      var columns = new List<string>();
      var current = new System.Text.StringBuilder();
      int depth = 0;

      foreach (char c in columnDefs)
      {
        if (c == '(' || c == '{' || c == '[')
        {
          depth++;
          current.Append(c);
        }
        else if (c == ')' || c == '}' || c == ']')
        {
          depth--;
          current.Append(c);
        }
        else if (c == ',' && depth == 0)
        {
          if (current.Length > 0)
          {
            columns.Add(current.ToString());
            current.Clear();
          }
        }
        else
        {
          current.Append(c);
        }
      }

      if (current.Length > 0)
      {
        columns.Add(current.ToString());
      }

      return columns;
    }
  }
  /// <summary>
  /// KQL data types mapped to CLR types
  /// </summary>
  public enum KqlDataType
  {
    Bool, // bool
    DateTime, // DateTime
    Dynamic, // Dictionary<string, object>
    Guid, // Guid
    Int, // int
    Long, // long
    Real, // double
    String, // string
    Timespan, // TimeSpan
    Decimal // decimal
  }

  /// <summary>
  /// Represents a KQL column definition
  /// </summary>
  public sealed class KqlColumn
  {
    public string Name { get; set; }

    public KqlDataType DataType { get; set; }

    public Type ClrType { get; set; }

    public bool IsNullable { get; set; }

    public string Description { get; set; }

    public override string ToString() => $"{Name}: {DataType}";
  }

  /// <summary>
  /// Represents a KQL table schema definition
  /// </summary>
  public sealed class KqlTableSchema
  {
    public string TableName { get; set; }

    public List<KqlColumn> Columns { get; set; } = new List<KqlColumn>();

    public string Description { get; set; }

    /// <summary>
    /// Get a column by name
    /// </summary>
    public KqlColumn GetColumn(string name) =>
        Columns.FirstOrDefault(c => c.Name.Equals(name, StringComparison.OrdinalIgnoreCase));

    /// <summary>
    /// Check if schema contains a column
    /// </summary>
    public bool HasColumn(string name) => GetColumn(name) != null;

    public override string ToString() =>
        $"{TableName} ({Columns.Count} columns): {string.Join(", ", Columns.Select(c => c.ToString()))}";
  }
}
