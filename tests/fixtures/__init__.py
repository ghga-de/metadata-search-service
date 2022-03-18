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

"""Fixtures that are used in both integration and unit tests"""

import json
import os

import pytest
from motor.motor_asyncio import AsyncIOMotorClient


@pytest.fixture(scope="function")
async def initialize_test_db():
    """Initialize a test metadata store using AsyncIOMotorClient"""
    curr_dir = os.path.dirname(__file__)
    json_files = [
        ("datasets.json", "Dataset"),
        ("studies.json", "Study"),
        ("experiments.json", "Experiment"),
        ("biospecimens.json", "Biospecimen"),
        ("samples.json", "Sample"),
    ]
    db_client = AsyncIOMotorClient("mongodb://localhost:27017")
    for filename, collection_name in json_files:
        full_filename = open(os.path.join(curr_dir, "data", filename))
        objects = json.load(full_filename)
        await db_client["metadata-store-test"][collection_name].delete_many({})
        await db_client["metadata-store-test"][collection_name].insert_many(
            objects[filename.split(".")[0]]
        )
        await db_client["metadata-store-test"][collection_name].create_index(
            [("$**", "text")]
        )
