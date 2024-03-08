# SPDX-License-Identifier: Apache-2.0
"""Tracks backend related options."""
import dataclasses


@dataclasses.dataclass
class BackendOptions:
    """Holds all the possible backend options."""
    backend_name: str

    def __init__(self):
        self.backend_name = 'duckdb'
