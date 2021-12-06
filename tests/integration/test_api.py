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

"""Test the api module"""

import nest_asyncio
import pytest
from fastapi import status
from fastapi.testclient import TestClient

from metadata_search_service.api.main import app
from metadata_search_service.config import Config, get_config
from tests.fixtures import initialize_test_db  # noqa: F401,F811

nest_asyncio.apply()


def get_config_override():
    return Config(db_url="mongodb://localhost:27017", db_name="metadata-store-test")


app.dependency_overrides[get_config] = get_config_override
client = TestClient(app)


def test_index():
    """Test the index endpoint"""

    client = TestClient(app)
    response = client.get("/")

    assert response.status_code == status.HTTP_200_OK
    assert response.text == '"Index for Metadata Search Service."'


@pytest.mark.parametrize(
    "query,document_type,facet",
    [
        ({"query": "*"}, "Dataset", False),
        ({"query": "*"}, "Study", True),
    ],
)
@pytest.mark.asyncio
async def test_search(initialize_test_db, query, document_type, facet):  # noqa: F811
    response = client.post(
        f"/rpc/search?document_type={document_type}&facet={facet}", json=query
    )
    assert response.status_code == 200
    data = response.json()
    assert len(data["hits"]) > 0
    if facet:
        assert len(data["facets"]) > 0
