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

"""Defines all dataclasses/classes pertaining to a data model or schema"""

from enum import Enum
from typing import Dict, List, Optional

from pydantic import BaseModel, Field


class DocumentType(str, Enum):
    """
    Enum for the type of document.
    """

    DATASET = "Dataset"
    PROJECT = "Project"
    STUDY = "Study"
    EXPERIMENT = "Experiment"
    SAMPLE = "Sample"
    BIOSPECIMEN = "Biospecimen"
    INDIVIDUAL = "Individual"
    PUBLICATION = "Publication"
    FILE = "File"


class FacetOption(BaseModel):
    """
    Represent values and their corresponding count for a facet.
    """

    option: str = Field(description="One of the values for the facet")
    count: Optional[int] = Field(
        None,
        description="The count that represents number of documents that has this facet value",
    )


class Facet(BaseModel):
    """
    Represents a facet and the possible values for that facet.
    """

    key: str = Field(description="The facet key")
    options: List[FacetOption] = Field(
        description="One or more values and their counts"
    )


class FilterOption(BaseModel):
    """
    Represents a Filter option.
    """

    key: str = Field(description="The filter key")
    value: str = Field(description="The filter value")


class SearchQuery(BaseModel):
    """
    Represents the Search Query.
    """

    query: str = Field(description="The query string to use for search")
    filters: Optional[List[FilterOption]] = Field(
        None, description="One or more filters to apply when performing a search"
    )


class SearchHit(BaseModel):
    """
    Represents the Search Hit.
    """

    document_type: DocumentType = Field(description="The type of document")
    id: str = Field(description="The unique identifier of the document")
    context: Optional[str] = Field(
        None, description="The context where this search hit was found"
    )
    content: Optional[Dict] = Field(
        None, description="The full document of the search hit"
    )


class SearchResult(BaseModel):
    """
    Represents the Search Result.
    """

    facets: List[Facet] = Field(
        description="One or more facets that summarizes the hits"
    )
    count: int = Field(description="Number of hits")
    hits: List[SearchHit] = Field(description="One or more search hits")
