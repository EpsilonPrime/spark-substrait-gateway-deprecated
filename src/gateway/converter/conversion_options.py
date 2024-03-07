# SPDX-License-Identifier: Apache-2.0
"""Tracks conversion related options."""
import dataclasses


@dataclasses.dataclass
class ConversionOptions:
    """Holds all the possible conversion options."""
    use_named_table_workaround: bool
    needs_scheme_in_path_uris: bool
    use_project_emit_workaround: bool
    use_project_emit_workaround2: bool
    use_emits_instead_of_direct: bool

    return_names_with_types: bool

    def __init__(self):
        self.use_named_table_workaround = False
        self.needs_scheme_in_path_uris = False
        self.use_project_emit_workaround = False
        self.use_project_emit_workaround2 = False
        self.use_emits_instead_of_direct = False

        self.return_names_with_types = False
