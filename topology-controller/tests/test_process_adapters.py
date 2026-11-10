from adapters.process import MonotonicClock, ResidentMemoryProbe


def test_the_monotonic_clock_never_goes_back():
    clock = MonotonicClock()
    first = clock.now()
    assert clock.now() >= first


def test_the_memory_probe_reports_a_resident_set_above_zero():
    assert ResidentMemoryProbe().resident_bytes() > 0
