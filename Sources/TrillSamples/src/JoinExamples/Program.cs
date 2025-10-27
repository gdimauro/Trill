// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Dynamic;
using System.Globalization;
using System.Linq;
using System.Reactive.Linq;
using System.Reflection;
using Microsoft.StreamProcessing;

namespace JoinExamples
{
  public sealed class Program
  {
    // Wrapper that stores arbitrary fields in a dictionary but exposes typed getters usable inside expression trees.
    public sealed class DynamicPayload
    {
      private readonly Dictionary<string, object> fields;
      public DynamicPayload(Dictionary<string, object> fields) => this.fields = fields;

      // public int GetInt(string name) => this.fields.TryGetValue(name, out var v) ? Convert.ToInt32(v) : 0;
      // public object Get(string name) => this.fields.TryGetValue(name, out var v) ? v : null;

      // Convenience strongly typed properties for common fields (avoid string each time).
      // public int Id => GetInt("id");
      // public int Type => GetInt("type");

      public T Get<T>(string name) => this.fields.TryGetValue(name, out var v) ? (T)Convert.ChangeType(v, typeof(T)) : default;

      public override string ToString() => string.Join(", ", this.fields.Select(kv => kv.Key + ":" + kv.Value));
    }

    // Helper to build a dynamic-like object then wrap into DynamicPayload.
    private static DynamicPayload CreatePayload(int id, int type)
        => new DynamicPayload(new Dictionary<string, object> { ["id"] = id, ["type"] = type });

    private static readonly List<StreamEvent<DynamicPayload>> dynamics1 =
    [
      StreamEvent.CreateInterval(1, 10, CreatePayload(1, 11)),
      StreamEvent.CreateInterval(1, 10, CreatePayload(2, 12)),
      StreamEvent.CreateInterval(3, 10, CreatePayload(3, 13)),
      StreamEvent.CreateInterval(3, 10, CreatePayload(1, 14)),
      StreamEvent.CreateInterval(5, 10, CreatePayload(2, 15)),
      StreamEvent.CreateInterval(5, 10, CreatePayload(3, 11)),
      StreamEvent.CreateInterval(7, 10, CreatePayload(1, 12)),
      StreamEvent.CreateInterval(7, 10, CreatePayload(2, 13)),
      StreamEvent.CreateInterval(9, 10, CreatePayload(3, 14)),
      // StreamEvent.CreatePunctuation<DynamicPayload>(StreamEvent.InfinitySyncTime)
     ];

    private static readonly List<StreamEvent<DynamicPayload>> dynamics2 =
    [
      StreamEvent.CreateInterval(2, 10, CreatePayload(1, 21)),
      StreamEvent.CreateInterval(4, 10, CreatePayload(2, 22)),
      StreamEvent.CreateInterval(6, 10, CreatePayload(3, 23)),
      StreamEvent.CreateInterval(8, 10, CreatePayload(4, 24)),
      // StreamEvent.CreatePunctuation<DynamicPayload>(StreamEvent.InfinitySyncTime)
    ];

    [DisplayName("CrossJoinExample")]
    private static void CrossJoinExample()
    {
      var input1 = dynamics1.ToObservable().ToStreamable();
      for (var i = 11; i < 200000; i += 1)
        dynamics1.Add(StreamEvent.CreateInterval(i, i + 10, CreatePayload(i % 4 + 1, i)));
      dynamics1.Add(StreamEvent.CreatePunctuation<DynamicPayload>(StreamEvent.InfinitySyncTime));
      // Console.WriteLine("Input1 =");
      // input1.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

      var input2 = dynamics2.ToObservable().ToStreamable();
      for (var i = 10; i < 200000; i += 2)
        dynamics2.Add(StreamEvent.CreateInterval(i, i + 10, CreatePayload(i % 4 + 1, i)));
      dynamics2.Add(StreamEvent.CreatePunctuation<DynamicPayload>(StreamEvent.InfinitySyncTime));
      // Console.WriteLine("Input2 =");
      // input2.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

      Console.WriteLine();
      Console.WriteLine("Query:");
      Console.WriteLine("    input1.Join(input2,(l,r)=> new { ID1 = l.Id), Type1 = l.Type), ID2 = r.Id), Type2 = r.Type) })");
      var output = input1.Join(
          input2,
          (left, right) => new { ID1 = left.Get<int>("id"), Type1 = left.Get<int>("type"), ID2 = right.Get<int>("id"), Type2 = right.Get<int>("type") });

      Console.WriteLine();
      Console.WriteLine("Output =");
      output.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();
    }

    [DisplayName("EquiJoinExample")]
    private static void EquiJoinExample()
    {
      var input1 = dynamics1.ToObservable().ToStreamable();
      Console.WriteLine("Input1 =");
      input1.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

      var input2 = dynamics2.ToObservable().ToStreamable();
      Console.WriteLine("Input2 =");
      input2.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

      Console.WriteLine();
      Console.WriteLine("Query:");
      Console.WriteLine("    input1.Join(input2, w=>w.Id), w=>w.Id), (l,r)=> new { ID = l.Id), Type1 = l.Type), Type2 = r.Type) })");
      var output = input1.Join(
          input2,
          w => w.Get<int>("id"),
          w => w.Get<int>("id"),
          (left, right) => new { ID = left.Get<int>("id"), Type1 = left.Get<int>("type"), Type2 = right.Get<int>("type") });

      Console.WriteLine();
      Console.WriteLine("Output =");
      output.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();
    }

    [DisplayName("AntiJoinExample")]
    private static void AntiJoinExample()
    {
      var input1 = dynamics1.ToObservable().ToStreamable();
      Console.WriteLine("Input1 =");
      input1.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

      var input2 = dynamics2.ToObservable().ToStreamable();
      Console.WriteLine("Input2 =");
      input2.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

      Console.WriteLine();
      Console.WriteLine("Query: input1.WhereNotExists(input2, w=>w.Id), w=>w.Id))");
      var output = input1.WhereNotExists(input2, w => w.Get<int>("id"), w => w.Get<int>("id"));

      Console.WriteLine();
      Console.WriteLine("Output =");
      output.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();
    }

    [DisplayName("OuterJoinExample")]
    private static void OuterJoinExample()
    {
      var input1 = dynamics1.ToObservable().ToStreamable();
      Console.WriteLine("Input1 =");
      input1.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

      var input2 = dynamics2.ToObservable().ToStreamable();
      Console.WriteLine("Input2 =");
      input2.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();

      Console.WriteLine();
      Console.WriteLine("Query:");
      Console.WriteLine("    input2.LeftOuterJoin(input1, w=>w.Id), w=>w.Id), w=> new { ID = w.Id), Type1 = w.Type), Type2 = 0 }, (l,r)=> new { ID = l.Id), Type1 = l.Type), Type2 = r.Type) })");
      var output = input2.LeftOuterJoin(
          input1,
          w => w.Get<int>("id"),
          w => w.Get<int>("id"),
          w => new { ID = w.Get<int>("id"), Type1 = w.Get<int>("type"), Type2 = 0 },
          (left, right) => new { ID = left.Get<int>("id"), Type1 = left.Get<int>("type"), Type2 = right.Get<int>("type") });

      Console.WriteLine();
      Console.WriteLine("Output =");
      output.ToStreamEventObservable().ForEachAsync(e => Console.WriteLine(e)).Wait();
    }

    private struct Function
    {
      public readonly MethodInfo Method;
      public readonly string Name;
      public Function(MethodInfo method, string name) { this.Method = method; this.Name = name; }
    }

    private static Function[] GetFunctions()
    {
      var functions = new List<Function>();
      foreach (var method in typeof(Program).GetMethods(BindingFlags.Static | BindingFlags.NonPublic))
      {
        var nameAttr = method.GetCustomAttribute<DisplayNameAttribute>();
        if (nameAttr == null) continue;
        functions.Add(new Function(method, nameAttr.DisplayName));
      }
      return functions.ToArray();
    }

    public static void Main(string[] args)
    {
      var demos = GetFunctions();
      while (true)
      {
        Console.WriteLine();
        Console.WriteLine("Pick an action:");
        for (int demo = 0; demo < demos.Length; demo++) Console.WriteLine($"{demo, 4} - {demos[demo].Name}");
        Console.WriteLine("Exit - Exit from Demo.");
        var response = Console.ReadLine().Trim();
        if (string.Equals(response, "exit", StringComparison.OrdinalIgnoreCase) || string.Equals(response, "e", StringComparison.OrdinalIgnoreCase)) break;
        int demoToRun; if (!int.TryParse(response, NumberStyles.Integer, CultureInfo.InvariantCulture, out demoToRun)) demoToRun = -1;
        if (demoToRun >= 0 && demoToRun < demos.Length)
        {
          Console.WriteLine();
          Console.WriteLine(demos[demoToRun].Name);
          demos[demoToRun].Method.Invoke(null, null);
        }
        else Console.WriteLine("Unknown Query Demo");
      }
    }
  }
}
