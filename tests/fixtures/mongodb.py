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

"""Fixture that setup and tears down a MongoDB database together with a correspondingly
configured app client."""

import json
import os
from dataclasses import dataclass

import pytest
from fastapi.testclient import TestClient
from testcontainers.mongodb import MongoDbContainer

from metadata_search_service.api.deps import get_config
from metadata_search_service.api.main import app
from metadata_search_service.config import Config

from . import BASE_DIR


@dataclass
class MongoAppFixture:
    app_client: TestClient
    config: Config


@pytest.fixture(scope="function")
def mongo_app_fixture():
    """
    Setup and tears down a MongoDB database together with a correspondingly
    configured app client.
    """

    json_files = [
        ("biospecimens.json", "Biospecimen"),
        ("data_access_committees.json", "DataAccessCommittee"),
        ("data_access_policies.json", "DataAccessPolicy"),
        ("datasets.json", "Dataset"),
        ("experiments.json", "Experiment"),
        ("files.json", "File"),
        ("individuals.json", "Individual"),
        ("projects.json", "Project"),
        ("samples.json", "Sample"),
        ("studies.json", "Study"),
        ("technologies.json", "Technology"),
    ]

    with MongoDbContainer() as mongodb:
        connection_url = mongodb.get_connection_url()
        db_client = mongodb.get_connection_client()
        config = Config(db_url=connection_url, db_name="test")

        for filename, collection_name in json_files:
            file_path = BASE_DIR / "test_data" / filename
            with open(file_path, "r", encoding="utf8") as file:
                file_content = json.load(file)
                objects = file_content[os.path.splitext(filename)[0]]
                db_client[config.db_name][collection_name].insert_many(objects)
            db_client[config.db_name][collection_name].create_index([("$**", "text")])

        app.dependency_overrides[get_config] = lambda: config
        app_client = TestClient(app)

        yield MongoAppFixture(app_client=app_client, config=config)
