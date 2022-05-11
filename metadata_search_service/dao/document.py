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
"""DAO for retrieving a document from the metadata store"""

import logging
from typing import Dict, List, Set, Tuple

from metadata_search_service.config import CONFIG, Config
from metadata_search_service.dao.db import get_db_client
from metadata_search_service.dao.utils import build_aggregation_query

# pylint: disable=too-many-locals, too-many-nested-blocks, too-many-arguments


async def get_documents(
    collection_name: str,
    search_query: str = "*",
    filters: List = None,
    facet_fields: Set = None,
    skip: int = 0,
    limit: int = 10,
    config: Config = CONFIG,
) -> Tuple[List[Dict], List[Dict], int]:
    """
    Get documents from a given ``collection_name``.

    Args:
        collection_name: The name of the collection from which to fetch the documents
        search_query: The search query string to use for text serach
        skip: The number of documents to skip
        facet_fields: A set of fields to facet on
        limit: The total number of documents to retrieve
        config: The config

    Returns:
        A list of documents from the collection, a list of facets,
        and a count that represents total number of hits

    """
    client = await get_db_client(config)
    collection = client[config.db_name][collection_name]
    if facet_fields:
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
    count = await _get_count(results)

    facets = []
    if facet_fields:
        for key in results.keys():
            if key not in {"data", "metadata"}:
                facet = {
                    key: sorted(results[key], key=lambda x: x["count"], reverse=True)
                }
                facets.append(facet)
    return docs, facets, count


async def _get_count(results: Dict) -> int:
    """
    Extract the total number of hits as reported by MongoDB
    Args:
        results: The results object from MongoDB
    Returns
        The total number of hits as reported by MongoDB
    """
    count = 0
    if "metadata" in results:
        if results["metadata"]:
            [total] = results["metadata"]
            count = total["total"]
    return count


async def _get_reference(
    document_id: str, collection_name: str, config: Config = CONFIG
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
    doc = await collection.find_one({"id": document_id})
    if doc:
        del doc["_id"]
    else:
        logging.warning(
            "Reference with ID %s not found in collection %s",
            document_id,
            collection_name,
        )
    return doc
