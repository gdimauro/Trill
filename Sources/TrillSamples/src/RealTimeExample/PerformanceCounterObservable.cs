// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics;
using System.Threading;

namespace RealTimeExample
{
    /// <summary>
    /// Struct that holds a sampled value with timestamp.
    /// </summary>
    internal struct PerformanceCounterSample
    {
        public DateTime StartTime;
        public float Value;

        public override string ToString() => $"{StartTime:O}:{Value}";
    }

    /// <summary>
    /// Cross-platform observable that periodically samples a metric.
    /// On Windows it will try to use the legacy PerformanceCounter if available, otherwise it falls back
    /// to a managed provider for common metrics (process CPU, working set, GC collections, etc.).
    /// </summary>
    internal sealed class PerformanceCounterObservable : IObservable<PerformanceCounterSample>
    {
        private readonly Func<float> sampleProvider;
        private readonly TimeSpan pollingInterval;

        public PerformanceCounterObservable(
            string categoryName,
            string counterName,
            string instanceName,
            TimeSpan pollingInterval)
        {
            this.sampleProvider = BuildSampleProvider(categoryName, counterName, instanceName);
            this.pollingInterval = pollingInterval;
        }

        public IDisposable Subscribe(IObserver<PerformanceCounterSample> observer) => new Subscription(this, observer);

        /// <summary>
        /// Build a sample provider. Attempts Windows PerformanceCounter first (if supported), then falls back
        /// to managed implementations for common counters. If no mapping exists returns a provider yielding 0.
        /// </summary>
        private static Func<float> BuildSampleProvider(string category, string counter, string instance)
        {
#if WINDOWS
            try
            {
                // Try legacy performance counter (still available on Windows).
                var pc = new PerformanceCounter(category, counter, instance, true);
                return () =>
                {
                    try { return pc.NextValue(); }
                    catch { return 0f; }
                };
            }
            catch
            {
                // Fall through to managed fallback.
            }
#endif
            // Managed fallbacks for common counters.
            // Extend mappings as needed.

            if (category.Equals("Process", StringComparison.OrdinalIgnoreCase) &&
                counter.Equals("Working Set", StringComparison.OrdinalIgnoreCase))
            {
                return () => (float)Process.GetCurrentProcess().WorkingSet64;
            }

            if (category.Equals("Process", StringComparison.OrdinalIgnoreCase) &&
                counter.Equals("% Processor Time", StringComparison.OrdinalIgnoreCase))
            {
                // Approximate process CPU % over the last interval. We calculate inside Subscription using deltas.
                // Return total processor time (ms) here; Subscription converts delta to percentage.
                return () => (float)Process.GetCurrentProcess().TotalProcessorTime.TotalMilliseconds;
            }

            if (category.Equals("GC", StringComparison.OrdinalIgnoreCase) &&
                counter.StartsWith("Gen ", StringComparison.OrdinalIgnoreCase) && counter.EndsWith(" Collections", StringComparison.OrdinalIgnoreCase))
            {
                // e.g. "Gen 0 Collections" -> extract generation number.
                if (int.TryParse(counter.AsSpan(4, 1), out int gen))
                {
                    return () => GC.CollectionCount(gen);
                }
            }

            // Default unmapped provider.
            return () => 0f;
        }

        private sealed class Subscription : IDisposable
        {
            private readonly Func<float> sampleProvider;
            private readonly TimeSpan pollingInterval;
            private readonly IObserver<PerformanceCounterSample> observer;
            private readonly Timer timer;
            private readonly object sync = new();
            private bool isDisposed;

            // State used for rate calculations (e.g. CPU % or counter deltas)
            private float previousRaw;
            private DateTime previousTimestamp;

            // Static helper must appear before instance members per StyleCop SA1204.
            private static float ComputeValue(float currentRaw, float previousRaw, TimeSpan elapsed)
            {
                if (elapsed <= TimeSpan.Zero) return currentRaw;
                if (currentRaw >= previousRaw && previousRaw >= 0)
                {
                    float delta = currentRaw - previousRaw;
                    double cpuMsPossible = elapsed.TotalMilliseconds * Environment.ProcessorCount;
                    if (cpuMsPossible > 0 && delta <= cpuMsPossible * 1.2)
                    {
                        return (float)(100.0 * delta / cpuMsPossible); // percentage utilization
                    }
                    return (float)(delta / elapsed.TotalSeconds); // generic rate
                }
                return currentRaw; // fallback
            }

            public Subscription(PerformanceCounterObservable observable, IObserver<PerformanceCounterSample> observer)
            {
                this.sampleProvider = observable.sampleProvider;
                this.pollingInterval = observable.pollingInterval;
                this.observer = observer;
                this.previousRaw = this.sampleProvider();
                this.previousTimestamp = DateTime.UtcNow;
                this.timer = new Timer(Sample);
                this.timer.Change(this.pollingInterval, Timeout.InfiniteTimeSpan);
            }

            private void Sample(object state)
            {
                lock (this.sync)
                {
                    if (this.isDisposed) return;

                    var now = DateTime.UtcNow;
                    float currentRaw = this.sampleProvider();
                    float value = ComputeValue(currentRaw, this.previousRaw, now - this.previousTimestamp);
                    this.observer.OnNext(new PerformanceCounterSample { StartTime = now, Value = value });
                    this.previousRaw = currentRaw;
                    this.previousTimestamp = now;
                    this.timer.Change(this.pollingInterval, Timeout.InfiniteTimeSpan);
                }
            }

            public void Dispose()
            {
                lock (this.sync)
                {
                    if (this.isDisposed) return;
                    this.isDisposed = true;
                    this.timer.Dispose();
                }
            }
        }
    }
}
