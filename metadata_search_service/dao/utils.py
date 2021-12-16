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
"""DAO specific utilities for the Metadata Search Service"""

from typing import Dict, List, Optional, Set

import stringcase

MAX_LIMIT = 100

NON_NESTED_FIELDS: Set = {"has_attribute"}


# pylint: disable=too-many-locals, too-many-arguments


def check_filter_field(field: str) -> bool:
    """
    Check if a given field is a nested field.

    Args:
        field: Field name

    Returns:
        Whether or not the field is a nested field.

    """
    if "." in field:
        top_level_field = field.split(".", 1)[0]
        if (
            top_level_field.startswith("has_")
            and top_level_field not in NON_NESTED_FIELDS
        ):
            return True
    return False


def get_nested_fields(fields: List) -> Set:
    """
    Get nested fields from a given set of fields.

    Args:
        fields: A list of fields

    Returns:
        A set of nested fields

    """
    nested_fields = set()
    for field in fields:
        if "." in field:
            top_level_field, nested_field = field.split(".", 1)
            nested_fields.add((top_level_field, nested_field))
    return nested_fields


def build_match_query(filters: List) -> Dict:
    """
    Build a match query for the MongoDB aggregation pipeline.

    Args:
        filters: A list of filters to use in the match query

    Returns:
        A dictionary that represents the match query

    """
    subpipelines: Dict = {}
    for query_filter in filters:
        if query_filter.key in subpipelines:
            subpipelines[query_filter.key]["$in"].append(query_filter.value)
        else:
            subpipelines[query_filter.key] = {"$in": [query_filter.value]}
    return subpipelines


def build_lookup_query(
    filters: Optional[List] = None, facet_fields: Optional[Set] = None
) -> List:
    """
    Build a lookup query for the MongoDB aggregation pipeline.

    Args:
        filters: A list of filters to use in the match query
        facet_fields: A list of fields to use for faceting

    Returns:
        A list that represents one or more lookup queries

    """
    subpipelines: List = []
    nested_fields = set()
    if filters:
        filter_fields = [x.key for x in filters]
        nested_fields.update(get_nested_fields(filter_fields))

    if facet_fields:
        nested_fields.update(get_nested_fields(list(facet_fields)))

    seen = set()
    for top_level_field, _ in nested_fields:
        lookup_pipeline = {}
        c_name = stringcase.pascalcase(top_level_field.split("has_", 1)[1])
        if c_name not in seen:
            seen.add(c_name)
            lookup_pipeline["from"] = c_name
            lookup_pipeline["localField"] = top_level_field
            lookup_pipeline["foreignField"] = "id"
            lookup_pipeline["as"] = top_level_field
            subpipelines.append(lookup_pipeline)
    return subpipelines


def build_facet_query(facet_fields: Set) -> Dict:
    """
    Build a facet query for the MongoDB aggregation pipeline.

    Args:
        facet_fields: A list of fields to use for faceting

    Returns:
        A dictionary that represents the facet query

    """
    subpipelines: Dict = {}
    for field in facet_fields:
        subpipelines[field.replace(".", "__")] = [
            {"$group": {"_id": f"${field}", "count": {"$sum": 1}}}
        ]
    return subpipelines


def build_text_search_query(query_string: str) -> Dict:
    """
    Build a text search query for the MongoDB aggregation pipeline.

    Args:
        query_string: A query string

    Returns:
        A dictionary that represents the text search query

    """
    return {"$text": {"$search": query_string}}


def build_projection_query(
    filters: Optional[List] = None, facet_fields: Optional[Set] = None
) -> Dict:
    """
    Build a projection query for the MongoDB aggregation pipeline
    that excludes _id field from the top level and all nested documents

    Args:
        filters: A list of filters used
        facet_fields: A set of fields used for faceting

    Returns:
        A dictionary that represents the projection query

    """
    subpipelines: Dict = {}
    nested_fields = set()
    if filters:
        filter_fields = [x.key for x in filters]
        nested_fields.update(get_nested_fields(filter_fields))
    if facet_fields:
        nested_fields.update(get_nested_fields(list(facet_fields)))
    subpipelines["data._id"] = 0
    for top_level_field, _ in nested_fields:
        subpipelines[f"data.{top_level_field}._id"] = 0
    return subpipelines


def build_aggregation_query(
    search_query: str = "*",
    filters: Optional[List] = None,
    facet_fields: Optional[Set] = None,
    skip: int = 0,
    limit: int = 10,
) -> List:
    """
    Build an aggregation query for the MongoDB aggregation pipeline,
    by generating the appropriate pipelines (and sub-pipelines) that
    can be used to query the underlying MongoDB store.

    Args:
        search_query: The search query string to use for text serach
        filters: A list of filters to use in the query
        facet_fields: A set of fields to use for faceting
        skip: The number of documents to skip
        limit: The total number of documents to retrieve

    Returns:
        A list that represents the projection query

    """
    pipelines: List = []
    if search_query and search_query not in {"*"}:
        # Text search with match
        text_search_query = build_text_search_query(search_query)
        match_pipeline = {"$match": text_search_query}
        pipelines.append(match_pipeline)

    if filters or facet_fields:
        # Perform lookup
        lookup_query = build_lookup_query(filters=filters, facet_fields=facet_fields)
        if lookup_query:
            for query in lookup_query:
                lookup_pipeline = {"$lookup": query}
                pipelines.append(lookup_pipeline)

    if filters:
        # Apply filters
        match_query = build_match_query(filters=filters)
        match_pipeline = {"$match": match_query}
        pipelines.append(match_pipeline)

    # Sort
    sort_pipeline = {"$sort": {"_id": 1}}
    pipelines.append(sort_pipeline)

    facet_query = {}
    if facet_fields:
        # Faceting
        facet_query = build_facet_query(facet_fields=facet_fields)

    # Pagination
    facet_query["metadata"] = [{"$count": "total"}]
    facet_query["data"] = [{"$skip": skip}, {"$limit": min(limit, MAX_LIMIT)}]

    facet_pipeline = {"$facet": facet_query}
    pipelines.append(facet_pipeline)

    # Projection
    projection_query = build_projection_query(
        filters=filters, facet_fields=facet_fields
    )
    projection_pipeline = {"$project": projection_query}
    pipelines.append(projection_pipeline)
    return pipelines
