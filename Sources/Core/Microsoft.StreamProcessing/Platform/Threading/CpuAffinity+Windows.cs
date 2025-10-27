using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Runtime.InteropServices;

namespace Microsoft.StreamProcessing.Platform.Threading;

/// <summary>
/// Cross platform CPU affinity helper for affinitizing the current thread to a specific CPU core.
/// </summary>
public partial class CpuAffinity
{
  // -------------------- Windows --------------------
  // Prefer SetThreadAffinityMask over iterating ProcessThread objects.
  [DllImport("kernel32.dll")]
  private static extern IntPtr GetCurrentThread();

  [DllImport("kernel32.dll", SetLastError = true)]
  private static extern IntPtr SetThreadAffinityMask(IntPtr hThread, IntPtr dwThreadAffinityMask);

  private static void SetWindowsThreadAffinity(int cpuIndex)
  {
    if (cpuIndex < 0 || cpuIndex >= Environment.ProcessorCount)
      throw new ArgumentOutOfRangeException(nameof(cpuIndex));

    // Use 64-bit mask (Windows supports up to 64 CPUs per group here).
    ulong mask = 1UL << cpuIndex;
    var prev = SetThreadAffinityMask(GetCurrentThread(), (IntPtr)unchecked((long)mask));
    if (prev == IntPtr.Zero)
      throw new Win32Exception(Marshal.GetLastWin32Error(), "SetThreadAffinityMask failed");
  }
}
