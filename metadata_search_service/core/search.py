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
"""Business logic for performing search on the metadata store"""

from typing import Dict, List, Tuple

from metadata_search_service.config import CONFIG, Config
from metadata_search_service.core.utils import DEFAULT_FACET_FIELDS, format_facet_key
from metadata_search_service.dao.document import get_documents

# pylint: disable=too-many-locals, too-many-nested-blocks, too-many-arguments


async def perform_search(
    document_type: str,
    search_query: str = "*",
    filters: List = None,
    return_facets: bool = False,
    skip: int = 0,
    limit: int = 10,
    config: Config = CONFIG,
) -> Tuple[List[Dict], List[Dict], int]:
    """
    Perform a search on the metadata store and get all
    documents that match a given search query.

    Args:
        document_type: The type of document
        search_query: The search query string to use for text serach
        return_facets: Whether or not to facet. Defaults to False
        skip: The number of documents to skip
        limit: The total number of documents to retrieve
        config: The config

    Returns:
        A list of documents, a list of facets (if ``return_facets=True``),
        and a count representing total number of hits

    """
    docs, facet_results, count = await get_documents(
        collection_name=document_type,
        search_query=search_query,
        filters=filters,
        facet_fields=DEFAULT_FACET_FIELDS[document_type],
        skip=skip,
        limit=limit,
        config=config,
    )
    hits = [{"document_type": document_type, "id": x["id"], "content": x} for x in docs]
    facets = []
    if return_facets and facet_results:
        for facet_result in facet_results:
            for key, value in facet_result.items():
                facet = {
                    "key": key.replace("__", "."),
                    "name": format_facet_key(document_type, key.replace("__", ".")),
                    "options": [],
                }
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
    return hits, facets, count
