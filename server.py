"""Omnicorp server."""
import logging
import os
import pickle
import re
from typing import List

import aioredis
import asyncpg
from fastapi import Depends, FastAPI, Query
from starlette.middleware.cors import CORSMiddleware

LOGGER = logging.getLogger(__name__)

POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', None)
POSTGRES_DB = os.getenv('POSTGRES_DB', 'postgres')

REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = os.getenv('REDIS_PORT', '6379')
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', None)

app = FastAPI(
    title='OmniCorp',
    description='Literature co-occurrence service',
    version='1.0.0',
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

LOGGER.setLevel(logging.DEBUG)


async def get_postgres():
    """Get Postgres connection."""
    conn = await asyncpg.connect(
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        database=POSTGRES_DB,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT
    )
    yield conn
    await conn.close()


async def get_redis():
    """Get Redis connection."""
    redis = await aioredis.create_redis_pool(
        f'redis://{REDIS_HOST}:{REDIS_PORT}',
        password=REDIS_PASSWORD,
    )
    yield redis
    redis.close()


def get_prefix(curie):
    """Get prefix from CURIE."""
    match = re.fullmatch('([a-zA-Z.]+):[A-Z0-9]+', curie)
    if match is None:
        raise ValueError(f'{curie} is not a valid CURIE')
    return match[1]


@app.get('/shared', response_model=int)
async def get_shared_pmids(
        curies: List[str] = Query(..., alias='curie'),
        postgres_conn=Depends(get_postgres),
        redis_conn=Depends(get_redis),
) -> int:
    """Get PMIDs shared by ids."""
    assert len(curies) < 10
    curies.sort()
    if len(curies) == 1:
        key = f'OmnicorpSupport({curies[0]})'
        value = await redis_conn.get(key)
        if value is not None:
            value = pickle.loads(value)
            LOGGER.debug('Got %s from cache.', key)
            return value['omnicorp_article_count']
    if len(curies) == 2:
        key = f'OmnicorpSupport_count({curies[0]},{curies[1]})'
        value = await redis_conn.get(key)
        if value is not None:
            value = pickle.loads(value)
            LOGGER.debug('Got %s from cache.', key)
            return value
    LOGGER.debug('Getting support for %s from PostgreSQL.', curies)
    prefixes = [get_prefix(curie) for curie in curies]
    statement = f"SELECT n00.pubmedid\n" + \
                f"FROM omnicorp.{prefixes[0]} n00\n"
    for idx, prefix in enumerate(prefixes[1:]):
        uid = f'n{idx + 1:02d}'
        statement += f"JOIN omnicorp.{prefix} {uid} ON n00.pubmedid = {uid}.pubmedid\n"
    conditions = '\nAND '.join([f'n{idx:02d}.curie = ${idx + 1}' for idx in range(len(prefixes))])
    statement += f"WHERE {conditions}"
    values = await postgres_conn.fetch(statement, *curies)
    num = len(values)
    if len(curies) == 1:
        key = f'OmnicorpSupport({curies[0]})'
        LOGGER.debug('Setting %s to %s.', key,
                     {'omnicorp_article_count': num})
        await redis_conn.set(key, pickle.dumps({'omnicorp_article_count': num}))
    elif len(curies) == 2:
        key = f'OmnicorpSupport_count({curies[0]},{curies[1]})'
        LOGGER.debug('Setting %s to %s.', key, num)
        await redis_conn.set(key, pickle.dumps(num))
    return num
