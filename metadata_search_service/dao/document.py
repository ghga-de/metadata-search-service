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
from typing import Dict, List, Tuple

from metadata_search_service.config import Config, get_config
from metadata_search_service.core.utils import (
    DEFAULT_FACET_FIELDS,
    MAX_LIMIT,
    build_aggregation_query,
)
from metadata_search_service.dao.db import get_db_client

# pylint: disable=too-many-locals, too-many-nested-blocks, too-many-arguments


async def get_documents(
    document_type: str,
    search_query: str = "*",
    filters: List = None,
    return_facets: bool = False,
    skip: int = 0,
    limit: int = 10,
    config: Config = get_config(),
) -> Tuple[List, List]:
    """
    Get documents for a given document type.

    Args:
        document_type: The type of document
        search_query: The search query string to use for text serach
        return_facets: Whether or not to facet. Defaults to False
        skip: The number of documents to skip
        limit: The total number of documents to retrieve
        config: The config

    Returns:
        A list of documents with facets and a list of facets, if ``return_facets=True``

    """
    docs, facet_results = await _get_documents(
        document_type,
        search_query=search_query,
        filters=filters,
        return_facets=return_facets,
        skip=skip,
        limit=min(limit, MAX_LIMIT),
        config=config,
    )
    hits = [{"document_type": document_type, "id": x["id"], "content": x} for x in docs]
    facets = []
    if return_facets and facet_results:
        for facet_result in facet_results:
            for key, value in facet_result.items():
                facet = {"key": key.replace("__", "."), "options": []}
                for val in value:
                    if val["_id"]:
                        if isinstance(val["_id"], str):
                            facet_key = val["_id"]
                        else:
                            facet_key = val["_id"][0]
                    else:
                        facet_key = str(val["_id"])
                    facet_option = {
                        "option": facet_key,
                        "count": val["count"],
                    }
                    facet["options"].append(facet_option)
                facets.append(facet)
    return hits, facets


async def _get_documents(
    collection_name: str,
    search_query: str = "*",
    filters: List = None,
    return_facets: bool = False,
    skip: int = 0,
    limit: int = 10,
    config: Config = get_config(),
) -> Tuple[List, List]:
    """
    Get documents from a given ``collection_name``.

    Args:
        collection_name: The name of the collection from which to fetch the documents
        search_query: The search query string to use for text serach
        skip: The number of documents to skip
        return_facets: Whether or not to facet. Defaults to False
        limit: The total number of documents to retrieve
        config: The config

    Returns:
        A list of documents from the collection and a list of facets

    """
    client = await get_db_client(config)
    collection = client[config.db_name][collection_name]
    if return_facets:
        facet_fields = DEFAULT_FACET_FIELDS[collection_name]
        query = build_aggregation_query(
            search_query=search_query,
            filters=filters,
            facet_fields=facet_fields,
            skip=skip,
            limit=limit,
        )
    else:
        query = build_aggregation_query(
            search_query=search_query, filters=filters, skip=skip, limit=limit
        )

    [results] = await collection.aggregate(query).to_list(None)
    docs = results["data"]

    facets = []
    if return_facets:
        for key in results.keys():
            if key not in {"data", "metadata"}:
                facet = {key: results[key]}
                facets.append(facet)
    return docs, facets


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
