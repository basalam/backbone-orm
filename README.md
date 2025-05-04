## Backbone ORM
Backbone ORM is a lightweight, asynchronous Object-Relational Mapper (ORM) for Python, built on top of the PyPika SQL query builder. It provides a clean and efficient interface for interacting with PostgreSQL databases, leveraging type hints and asynchronous programming to enable scalable and maintainable database operations.

#### Features
- Asynchronous Support: Built with asyncio to support non-blocking database operations.
- Type-Hinted Models: Utilizes Python's type hints for defining models, enhancing code clarity and editor support.
- PostgreSQL Integration: Specifically designed for PostgreSQL databases, with support for connection pooling and schema management.
- Redis Integration: Includes support for Redis, allowing for caching and other in-memory data storage solutions.
- Flexible Querying: Provides a flexible query builder and supports soft deletes and model relationships.

#### Requirements 

- python 3.10+
- pypika 0.48+
- pydantic 2.0+
- basalam.backbone-redis-cache 0.0.11+

#### Installation & Upgrade

```shell
pip install basalam.backbone-orm --upgrade
```

### Usage

#### Define Models
```python
from basalam.backbone_orm import ModelAbstract

class UserModel(ModelAbstract):
    id: int
    name: str

```

#### Create Repositories
```python
import typing
import aioredis
from basalam.backbone_orm import (
    T,
    DriverEnum,
    PostgresManager,
    ConnectionConfig,
    RepositoryAbstract,
)

postgres = PostgresManager(
    default=DriverEnum.POOL,
    config=ConnectionConfig(...)
)

redis = aioredis.Redis(...)


class UserRepo(RepositoryAbstract[UserModel]):

    @classmethod
    async def connection(cls) -> PostgresConnection:
        return await postgres.acquire()

    @classmethod
    def redis(cls) -> aioredis.Redis:
        return redis

    @classmethod
    def table_name(cls) -> str:
        return "users"

    @classmethod
    def model(cls) -> typing.Type[T]:
        return UserModel

    @classmethod
    def soft_deletes(cls) -> bool:
        return True

    @classmethod
    def default_relations(cls) -> typing.List[str]:
        return []

```

#### Perform Queries
```python
user = await UserRepo.find_by_id(1)
```

#### Testing

```bash
# install pytest
pip install pytest

# run tests
python -m pytest
```

#### Changelog

- 0.0.11: Build and push process now handled by GitLab CI
- 0.0.13: fix - Correct return type of `update_return` method
- 0.0.14: Add support for custom order enums
- 0.0.15: Introduce `has_relations` in `ModelAbstract`
- 1.0.0: Introduce `QueryBuilder` and `Connection Manager`
- 1.0.9: Extend `QueryBuilderAbstract` from PyPika's `PostgreSQLQueryBuilder`
- 2.0.0: Drop support for Pydantic v1 and ensure compatibility with Pydantic v2
- 2.0.6: Add `with_thrashed` option to `find_by_id` method
- 2.0.14: Fix `dict` method in `ModelAbstract` for Pydantic v2 compatibility
