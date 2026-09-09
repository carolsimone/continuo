"""Manifest-source adapters.

Concrete sources (S3Source) implement service.ports.ManifestSourcePort; the
port lives in the application layer, not here, so the parse handler depends on
the abstraction while this package only provides implementations of it.
"""
