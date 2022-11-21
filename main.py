import argparse
import asyncio
import json
from typing import Any

import asyncpg

from parse_users import parse


def get_config(filename: str = "config.json") -> dict[str, Any]:
    with open(filename, 'r') as f:
        return json.load(f)


async def create_pool(config: dict[str, str | int]) -> asyncpg.Pool:
    return await asyncpg.create_pool(
        user=config['user'],
        password=config['password'],
        database=config['database'],
        host=config['host'],
        port=config['port']
    )


async def main() -> None:
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--config', type=str, default='config.json')
    args = arg_parser.parse_args()
    config = get_config(args.config)

    pool = await create_pool(config['database'])
    await parse(config.get("users", []), pool)


if __name__ == '__main__':
    asyncio.run(main())
