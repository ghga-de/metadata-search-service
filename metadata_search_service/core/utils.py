# Copyright 2021 - 2022 Universität Tübingen, DKFZ and EMBL
# for the German Human Genome-Phenome Archive (GHGA)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Core utilities for the Metadata Search Service"""

import time
from typing import Any, Dict, Set

DEFAULT_FACET_FIELDS: Dict[str, Set[Any]] = {
    "Dataset": {"type", "has_study.type"},
    "Project": set(),
    "Study": {"type"},
    "Experiment": {"type"},
    "Biospecimen": {"has_phenotypic_feature.name"},
    "Sample": set(),
    "Publication": set(),
    "File": {"format"},
    "Individual": {"gender", "has_phenotypic_feature.name"},
}


def get_time_in_millis() -> int:
    """
    Get current time in milliseconds.

    Returns:
        Time in milliseconds
    """
    return int(round(time.time() * 1000))


def format_facet_key(
    key: str, top_level_document_type: str = None, separator: str = " "
):
    """
    Format a facet key for better readability.

    Args:
        key: The facet key
        top_level_document_type: The top level document type
        separator: The separator to use between field names

    Returns:
        The formatted facet key
    """
    formatted_fields = []
    if top_level_document_type:
        formatted_fields.append(top_level_document_type.title())
    fields = key.split(".")
    for field in fields:
        if field.startswith("has_"):
            # reference field
            formatted_field = field.replace("has_", "")
            formatted_fields.append(formatted_field.title())
        else:
            formatted_fields.append(field)
    return separator.join(formatted_fields)
