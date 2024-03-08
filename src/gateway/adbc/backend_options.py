# SPDX-License-Identifier: Apache-2.0
"""Tracks backend related options."""
import dataclasses
from enum import Enum


class Backend(Enum):
    ARROW = 1
    DATAFUSION = 2
    DUCKDB = 3


@dataclasses.dataclass
class BackendOptions:
    """Holds all the possible backend options."""
    backend: Backend

    def __init__(self):
        self.backend = Backend.DUCKDB
