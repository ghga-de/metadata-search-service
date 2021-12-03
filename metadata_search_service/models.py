# Copyright 2021 Universität Tübingen, DKFZ and EMBL
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

"""Defines all dataclasses/classes pertaining to a data model or schema"""

from enum import Enum
from typing import Dict, List, Optional

from pydantic import BaseModel


class DocumentType(str, Enum):
    """
    Enum for the type of document.
    """

    DATASET = "Dataset"
    PROJECT = "Project"
    STUDY = "Study"
    EXPERIMENT = "Experiment"
    SAMPLE = "Sample"
    PUBLICATION = "Publication"
    FILE = "File"


class FacetOption(BaseModel):
    """
    Represent values and their corresponding count for a facet.
    """

    option: str
    count: Optional[int] = None


class Facet(BaseModel):
    """
    Represents a facet and the possible values for that facet.
    """

    key: str
    options: List[FacetOption]


class SearchQuery(BaseModel):
    """
    Represents the Search Query.
    """

    query: str
    facets: Optional[Dict] = None


class SearchHit(BaseModel):
    """
    Represents the Search Hit.
    """

    document_type: DocumentType
    id: str
    context: Optional[str] = None
    content: Optional[Dict] = None


class SearchResult(BaseModel):
    """
    Represents the Search Result.
    """

    hits: List[SearchHit]
    facets: List[Facet]
