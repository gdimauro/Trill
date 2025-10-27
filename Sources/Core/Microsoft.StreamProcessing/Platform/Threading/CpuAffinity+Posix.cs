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
  // -------------------- Linux / POSIX --------------------
  // sched_setaffinity(pid=0 => calling thread), cpuset is an opaque byte array (cpu_set_t).
  [DllImport("libc", SetLastError = true)]
  private static extern int sched_setaffinity(int pid, IntPtr cpusetsize, IntPtr cpuset);

  private static void SetPosixThreadAffinity(int cpuIndex)
  {
    if (cpuIndex < 0 || cpuIndex >= Environment.ProcessorCount)
      throw new ArgumentOutOfRangeException(nameof(cpuIndex));

    // cpu_set_t as a byte array large enough for common CPU counts.
    // 128 bytes covers up to 1024 CPUs (8 bits/byte).
    const int CpuSetBytes = 128;
    var mask = new byte[CpuSetBytes];
    int byteIndex = cpuIndex / 8;
    int bitIndex = cpuIndex % 8;
    mask[byteIndex] |= (byte)(1 << bitIndex);

    // Pin and call sched_setaffinity for the *current thread* (pid=0).
    unsafe
    {
      fixed (byte* p = mask)
      {
        int rc = sched_setaffinity(0, (IntPtr)CpuSetBytes, (IntPtr)p);
        if (rc != 0)
          throw new Win32Exception(Marshal.GetLastWin32Error(), "sched_setaffinity failed");
      }
    }
  }
}
