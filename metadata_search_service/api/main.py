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

"""
Module containing the main FastAPI router and (optionally) top-level API enpoints.
Additional endpoints might be structured in dedicated modules
(each of them having a sub-router).
"""

from fastapi import Depends, FastAPI, HTTPException
from ghga_service_chassis_lib.api import configure_app

from metadata_search_service.api.deps import get_config
from metadata_search_service.config import CONFIG, Config
from metadata_search_service.dao.document import get_documents
from metadata_search_service.models import DocumentType, SearchQuery, SearchResult

# pylint: disable=too-many-arguments

app = FastAPI()
configure_app(app, config=CONFIG)


@app.get("/", summary="Index for Metadata Search Service")
async def index():
    """Index for Metadata Search Service."""
    return "Index for Metadata Search Service."


@app.post(
    "/rpc/search",
    summary="Search metadata by keywords and facets",
    response_model=SearchResult,
)
async def search(
    query: SearchQuery,
    document_type: DocumentType,
    return_facets: bool = False,
    skip: int = 0,
    limit: int = 10,
    config: Config = Depends(get_config),
):
    """Search metadata based on a given query string."""
    if query.query != "*":
        raise HTTPException(
            status_code=400,
            detail="Unexpected search query pattern."
            + " Only generic queries (`*`) are supported.",
        )
    hits, facets = await get_documents(
        document_type=document_type,
        return_facets=return_facets,
        skip=skip,
        limit=limit,
        config=config,
    )
    response = {"facets": facets, "hits": hits}
    return response
