"""The continuo-contracts library this process runs on: its version and the one validator
every release checks its contracts with.

The version is the one installed, which `pyproject.toml` pins exactly. A producer's bundle
names the version it was written with, and the parse refuses one newer than this.
"""
import importlib.metadata
from typing import Final

from service.contract_validation import ContractValidator

LIBRARY_VERSION: Final = importlib.metadata.version("continuo-contracts")
VALIDATOR: Final = ContractValidator()
