"""Adapters over the running process: its clock and its memory."""
import os
import resource
import time

_STATM = "/proc/self/statm"


class MonotonicClock:
    """Implements service.ports.ClockPort."""

    def now(self) -> float:
        return time.perf_counter()


class ResidentMemoryProbe:
    """Implements service.ports.MemoryProbePort.

    On Linux it reads the process's current resident set from /proc/self/statm.
    Where that file does not exist (a macOS host running the benchmark) it
    returns the process's peak resident set so far, which macOS reports in
    bytes through getrusage.
    """

    def resident_bytes(self) -> int:
        try:
            with open(_STATM) as statm:
                resident_pages = int(statm.read().split()[1])
        except FileNotFoundError:
            return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return resident_pages * os.sysconf("SC_PAGE_SIZE")
