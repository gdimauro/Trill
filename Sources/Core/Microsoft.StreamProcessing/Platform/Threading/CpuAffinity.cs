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
  /// <summary>
  /// Affinitize the current thread to the specified CPU core index.
  /// </summary>
  /// <param name="cpuIndex"></param>
  public static void AffinitizeCurrentThread(int cpuIndex)
  {
    if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
    {
      SetWindowsThreadAffinity(cpuIndex);
    }
    else
    {
      SetPosixThreadAffinity(cpuIndex);
    }
  }


}
