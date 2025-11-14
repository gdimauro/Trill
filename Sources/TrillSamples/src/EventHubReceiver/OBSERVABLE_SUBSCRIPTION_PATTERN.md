# Observable Subscription Pattern - Quick Reference

## The Problem

When using Trill with Rx observables and subjects, you can easily create a **deadlock** if you subscribe incorrectly.

### ? Wrong Pattern (Hangs)

```csharp
var subject = new Subject<StreamEvent<MyEvent>>();
var stream = subject.ToStreamable();
var query = stream.Where(x => x.Value > 10);

// This BLOCKS waiting for the stream to complete
query.ToStreamEventObservable().ForEachAsync(e => 
{
    Console.WriteLine(e.Payload);
}).Wait();  // ? HANGS HERE!

// Events are sent AFTER the Wait(), so they never arrive
foreach (var evt in events)
{
    subject.OnNext(evt);  // Never reached!
}
subject.OnCompleted();
```

**Why it hangs**: `ForEachAsync().Wait()` blocks the current thread waiting for the observable to complete. But the observable can't complete because you haven't sent the completion signal yet (it's after the `Wait()`).

## ? Correct Pattern

```csharp
var subject = new Subject<StreamEvent<MyEvent>>();
var stream = subject.ToStreamable();
var query = stream.Where(x => x.Value > 10);

// Step 1: Subscribe FIRST (non-blocking)
var subscription = query.ToStreamEventObservable().Subscribe(e => 
{
    if (e.IsData)
    {
        Console.WriteLine($"Received: {e.Payload}");
    }
});

// Step 2: THEN send events
foreach (var evt in events)
{
    subject.OnNext(evt);
}

// Step 3: Signal completion
subject.OnCompleted();

// Step 4: Wait for processing to finish
Thread.Sleep(1000); // Or use a more sophisticated completion signal

// Step 5: Clean up
subscription.Dispose();
```

## Key Principles

### 1. Use Subscribe, Not ForEachAsync().Wait()

**Non-blocking (Good)**:
```csharp
var subscription = observable.Subscribe(e => { /* handle event */ });
```

**Blocking (Causes hangs)**:
```csharp
observable.ForEachAsync(e => { /* handle event */ }).Wait();
```

### 2. Order Matters

```
???????????????????????????????????????
? 1. Create subjects and streamables  ?
???????????????????????????????????????
                  ?
???????????????????????????????????????
? 2. Build your query pipeline        ?
???????????????????????????????????????
                  ?
???????????????????????????????????????
? 3. Subscribe (NON-BLOCKING!)        ? ? Critical!
???????????????????????????????????????
                  ?
???????????????????????????????????????
? 4. Send events to subjects          ?
???????????????????????????????????????
                  ?
???????????????????????????????????????
? 5. Complete subjects                ?
???????????????????????????????????????
                  ?
???????????????????????????????????????
? 6. Wait for processing (optional)   ?
???????????????????????????????????????
                  ?
???????????????????????????????????????
? 7. Dispose subscriptions            ?
???????????????????????????????????????
```

### 3. Multiple Streams (Joins)

When joining multiple streams, subscribe BEFORE sending to ANY subject:

```csharp
var actionSubject = new Subject<StreamEvent<UserAction>>();
var profileSubject = new Subject<StreamEvent<UserProfile>>();

var actionStream = actionSubject.ToStreamable();
var profileStream = profileSubject.ToStreamable();

var joined = actionStream.Join(profileStream, ...);

// Subscribe FIRST
var subscription = joined.ToStreamEventObservable().Subscribe(e => { ... });

// THEN send to both subjects
foreach (var action in actions) actionSubject.OnNext(action);
foreach (var profile in profiles) profileSubject.OnNext(profile);

// Complete both
actionSubject.OnCompleted();
profileSubject.OnCompleted();

// Wait and cleanup
Thread.Sleep(1000);
subscription.Dispose();
```

## Common Scenarios

### Scenario 1: Simple Filter/Select

```csharp
var subject = new Subject<StreamEvent<SensorReading>>();
var stream = subject.ToStreamable();

var highTemps = stream.Where(s => s.Temperature > 80);

// Subscribe first
var sub = highTemps.ToStreamEventObservable().Subscribe(e => 
{
    if (e.IsData) Console.WriteLine($"Hot! {e.Payload.Temperature}°C");
});

// Send events
foreach (var reading in sensorData)
    subject.OnNext(reading);

subject.OnCompleted();
Thread.Sleep(500);
sub.Dispose();
```

### Scenario 2: Windowed Aggregation

```csharp
var subject = new Subject<StreamEvent<Transaction>>();
var stream = subject.ToStreamable();

var stats = stream
    .TumblingWindowLifetime(TimeSpan.FromSeconds(5).Ticks)
    .Aggregate(
        w => w.Count(),
        w => w.Sum(t => t.Amount),
        (count, sum) => new { Count = count, Total = sum });

// Subscribe first
var sub = stats.ToStreamEventObservable().Subscribe(e => 
{
    if (e.IsData) 
        Console.WriteLine($"Window: {e.Payload.Count} txns, ${e.Payload.Total}");
});

// Send events
foreach (var txn in transactions)
    subject.OnNext(txn);

subject.OnCompleted();
Thread.Sleep(2000);
sub.Dispose();
```

### Scenario 3: Join Two Streams

```csharp
var ordersSubject = new Subject<StreamEvent<Order>>();
var customersSubject = new Subject<StreamEvent<Customer>>();

var orders = ordersSubject.ToStreamable();
var customers = customersSubject.ToStreamable();

var enriched = orders.Join(
    customers,
    o => o.CustomerId,
    c => c.CustomerId,
    (o, c) => new { Order = o, Customer = c });

// Subscribe BEFORE sending to either stream
var sub = enriched.ToStreamEventObservable().Subscribe(e => 
{
    if (e.IsData)
        Console.WriteLine($"{e.Payload.Customer.Name} ordered {e.Payload.Order.Product}");
});

// Now send events
foreach (var order in orderList) ordersSubject.OnNext(order);
foreach (var customer in customerList) customersSubject.OnNext(customer);

ordersSubject.OnCompleted();
customersSubject.OnCompleted();

Thread.Sleep(1000);
sub.Dispose();
```

## Debugging Tips

### Symptom: Application hangs with no output

**Likely cause**: Blocked on `ForEachAsync().Wait()` before events are sent.

**Solution**: 
1. Remove `.Wait()` from `ForEachAsync()`
2. Use `Subscribe()` instead
3. Move subscription BEFORE sending events

### Symptom: No results printed

**Possible causes**:
1. Events sent before subscription established
2. Subjects completed before events sent
3. Time window not triggered

**Solutions**:
- Verify subscription happens first
- Add delays between event sends for time-based windows
- Check that `OnCompleted()` is called
- Add diagnostic output in subscription handler

### Symptom: Partial results

**Possible cause**: Race condition between event sending and processing

**Solution**:
- Add appropriate delays (e.g., `Thread.Sleep(1000)`) after `OnCompleted()`
- For production, use proper synchronization (ManualResetEvent, etc.)

## Production Patterns

For production code, use proper completion signals:

```csharp
var completed = new ManualResetEventSlim(false);
var results = new List<MyResult>();

var subscription = query.ToStreamEventObservable().Subscribe(
    onNext: e => 
    {
        if (e.IsData) results.Add(e.Payload);
    },
    onCompleted: () => 
    {
        completed.Set(); // Signal completion
    });

// Send events
foreach (var evt in events)
    subject.OnNext(evt);

subject.OnCompleted();

// Wait for actual completion
completed.Wait(TimeSpan.FromSeconds(10));
subscription.Dispose();

// Now results is fully populated
return results;
```

## Summary

? **DO**:
- Use `Subscribe()` for non-blocking observation
- Subscribe BEFORE sending events
- Complete subjects after all events sent
- Dispose subscriptions when done

? **DON'T**:
- Use `ForEachAsync().Wait()` with subjects
- Send events before subscribing
- Forget to call `OnCompleted()`
- Leave subscriptions undisposed

This pattern applies to all Trill examples that use Rx observables with subjects!
