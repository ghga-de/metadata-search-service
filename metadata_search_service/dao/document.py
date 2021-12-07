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
"""DAO for retrieving a document from the metadata store"""

import logging
from functools import lru_cache
from typing import Any, Dict, List, Set, Tuple

import stringcase
from pymongo import ASCENDING

from metadata_search_service.config import Config, get_config
from metadata_search_service.core.utils import DEFAULT_FACET_FIELDS
from metadata_search_service.dao.db import get_db_client

# pylint: disable=too-many-locals, too-many-nested-blocks, too-many-branches

MAX_LIMIT = 100


async def get_documents(
    document_type: str,
    return_facets: bool = False,
    skip: int = 0,
    limit: int = 10,
    config: Config = get_config(),
) -> Tuple[List, List]:
    """
    Get documents for a given document type.

    Args:
        document_type: The type of document
        return_facets: Whether or not to facet. Defaults to False
        skip: The number of documents to skip
        limit: The total number of documents to retrieve
        config: The config

    Returns:
        A list of documents with facets and a list of facets, if ``return_facets=True``

    """
    docs, facet_results = await _get_documents(
        document_type,
        return_facets=return_facets,
        skip=skip,
        limit=min(limit, MAX_LIMIT),
        config=config,
    )
    hits = [{"document_type": document_type, "id": x["id"], "content": x} for x in docs]
    facets = []
    if return_facets and facet_results:
        facet_result = facet_results[0]
        for key, value in facet_result.items():
            facet = {
                "key": key.replace("__", "."),
                "options": [
                    {"option": x["_id"] if x["_id"] else "", "count": x["count"]}
                    for x in value
                ],
            }
            facets.append(facet)
    return hits, facets


async def _get_documents(
    collection_name: str,
    return_facets: bool = False,
    skip: int = 0,
    limit: int = 10,
    config: Config = get_config(),
) -> Tuple[List, List]:
    """
    Get documents from a given ``collection_name``.

    Args:
        collection_name: The name of the collection from which to fetch the documents
        skip: The number of documents to skip
        return_facets: Whether or not to facet. Defaults to False
        limit: The total number of documents to retrieve
        config: The config

    Returns:
        A list of documents from the collection and a list of facets

    """
    client = await get_db_client(config)
    collection = client[config.db_name][collection_name]
    docs = await collection.find({}, {"_id": 0}).sort("id", ASCENDING).skip(skip).limit(limit).to_list(None)  # type: ignore
    facets = []
    if return_facets:
        facet_fields = DEFAULT_FACET_FIELDS[collection_name]
        facets = {}
        if facet_fields:
            query = _build_aggregation_query(facet_fields=facet_fields)
            print(f"[_get_documents_new] facet query: {query}")
            facets = await collection.aggregate(query).to_list(None)
            print(f"[_get_documents_new] facets: {facets}")
    return docs, facets


def _build_aggregation_query(
    search_query: str = "", filter_fields: Set = {}, facet_fields: Set = {}
) -> str:
    """
    Build an aggregation query by generating pipelines and sub-pipelines that
    can be used to query the underlying MongoDB store.

    """
    query_template = []
    if facet_fields:
        facet_pipeline_template = {"$facet": {}}
        for field in facet_fields:
            if "." not in field:
                # field is a top level field
                sub_pipeline = {
                    field: [{"$unwind": f"${field}"}, {"$sortByCount": f"${field}"}]
                }
            else:
                # field is a nested field
                top_level_field, nested_field = field.split(".", 1)
                sub_pipeline = {
                    field.replace(".", "__"): [
                        {"$unwind": f"${top_level_field}"},
                        {"$sortByCount": f"${nested_field}"},
                    ]
                }
            facet_pipeline_template["$facet"].update(sub_pipeline)
        query_template.append(facet_pipeline_template)
    return query_template


@lru_cache()
async def _get_reference(
    document_id: str, collection_name: str, config: Config = get_config()
) -> Dict:
    """
    Given a document ID and a collection name, query the metadata store
    and return the document.

    Args:
        document_id: The ID of the document
        collection_name: The collection in the metadata store that has the document

    Returns
        The document corresponding to ``document_id``
    """
    client = await get_db_client(config)
    collection = client[config.db_name][collection_name]
    doc = await collection.find_one({"id": document_id})  # type: ignore
    if doc:
        del doc["_id"]
    else:
        logging.warning(
            "Reference with ID %s not found in collection %s",
            document_id,
            collection_name,
        )
    return doc


async def generate_stats(
    documents: List, fields: Set, prefix: str = None
) -> Dict[str, Dict]:
    """
    Given a set of fields, count occurrence of values for each field
    in a given set of documents.

    Args:
        documents: A list of documents
        fields: A set of fields
        prefix (str, optional): Prefix for the stat key. Defaults to None

    Returns:
        A stats dictionary
    """
    stats: Dict[str, Dict] = {}
    for field in fields:
        # for each field create a stats dictionary
        if prefix:
            key = f"{prefix}.{field}"
        else:
            key = field
        stats[key] = {}
        for document in documents:
            # check if field exists in each document
            # and if it does, count the occurrence of the value
            # and store in stats dictionary
            if field in document:
                value = document[field]
                if isinstance(value, str):
                    # value is of type str
                    if value not in stats[key]:
                        stats[key][value] = 1
                    else:
                        stats[key][value] += 1
                elif isinstance(value, (list, tuple, set)):
                    # value is of type list, tuple, or set. i.e. multivalued
                    for item in value:
                        if item not in stats[key]:
                            stats[key][item] = 1
                        else:
                            stats[key][item] += 1
    return stats


async def generate_facets(  # noqa: C901
    documents: List[Dict], facet_fields: Set, prefix: str = None
) -> Dict:
    """
    Given a set of documents and a list of fields, generate the
    appropriate facets.

    Args:
        documents: A list of documents
        facet_fields: Fields to facet on
        prefix: Prefix for the facet key. Defaults to None

    Returns:
        A dictionary that represents facets generated on the given set of documents

    """
    top_level_fields = set()
    nested_fields = set()
    for field in facet_fields:
        if "." not in field:
            # top level fields type
            top_level_fields.add(field)
        else:
            # nested fields like dataset.has_experiment.type
            nested_fields.add(field)

    stats: Dict[str, Dict] = await generate_stats(documents, top_level_fields, prefix)

    if nested_fields:
        # process the next level of the document
        field_to_nested_fields: Dict[str, Set] = {}
        for nested_field_item in nested_fields:
            field, nested_field = nested_field_item.split(".", 1)
            if field in field_to_nested_fields:
                field_to_nested_fields[field].add(nested_field)
            else:
                field_to_nested_fields[field] = {nested_field}
        for field, nested_fields in field_to_nested_fields.items():
            nested_documents = []
            for document in documents:
                if field in document.keys():
                    nested_document: Any = document[field]
                    if isinstance(nested_document, dict):
                        nested_documents.append(nested_document)
                    elif isinstance(nested_document, (list, tuple, set)):
                        # one to many
                        if isinstance(nested_document[0], str):  # type: ignore
                            # one to many references
                            for doc_id in nested_document:
                                embedded_document = await _get_reference(
                                    doc_id,
                                    stringcase.pascalcase(field.split("_", 1)[1]),
                                )
                                nested_documents.append(embedded_document)
                        elif isinstance(nested_document[0], (list, tuple, set)):  # type: ignore
                            # one to many objects
                            nested_documents.extend(nested_document)
                    elif isinstance(nested_document, str):
                        # reference to another doc
                        embedded_document = await _get_reference(
                            nested_document,
                            stringcase.pascalcase(field.split("_", 1)[1]),
                        )
                        nested_documents.append(embedded_document)
            nested_stats = await generate_facets(
                nested_documents, nested_fields, prefix=field
            )
            stats.update(nested_stats)
    return stats
