#### Requirements

- Python 3.6+

#### Installation & Upgrade

```shell
pip install backbone-orm --extra-index-url https://repo.basalam.dev/artifactory/api/pypi/basalam-pypi-local/simple --upgrade
```

#### Usage

```python
from backbone_orm import (
    RepositoryAbstract,
    PostgresManager,
    DriverEnum,
ConnectionConfig,
    ModelAbstract,
    T,
    Parameters
)
import aioredis
import typing

postgres = PostgresManager(
    default=DriverEnum.POOL,
    config=ConnectionConfig(...)
)

redis = aioredis.Redis(...)


class UserModel(ModelAbstract):
    id: int
    name: str


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


print(await UserRepo.find_by_id(1))
```

#### Testing

```bash
# install pytest
pip install pytest

# run tests
python -m pytest
```

#### Changelog

- 0.0.11 Now build and push are done using gitlab-ci
- 0.0.13 fix: return type of update_return
- 0.0.14 custom order enums
- 0.0.15 has_relations in ModelAbstract
- 1.0.0 Adds QueryBuilder, Adds Connection Manager