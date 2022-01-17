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

"""Connects to database."""

from motor.motor_asyncio import AsyncIOMotorClient

from metadata_search_service.config import CONFIG, Config


async def get_db_client(config: Config = CONFIG) -> AsyncIOMotorClient:
    """
    Get database client.
    """
    db_url = config.db_url
    db_client = AsyncIOMotorClient(db_url)
    return db_client
