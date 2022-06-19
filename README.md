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
    PostgresConnection,
    ModelAbstract,
    T,
    Parameters
)
import aioredis
import typing

connection = PostgresConnection(
    user="root",
    password="root",
    host="127.0.0.1",
    port=1111,
    database="postgre",
    default_schema="postgre",
    timeout_in_seconds=5,
)

redis = aioredis.Redis(host="127.0.0.1", port=6379)


class UserModel(ModelAbstract):
    id: int
    name: str


class UserRepo(RepositoryAbstract):
    @classmethod
    def connection(cls) -> PostgresConnection:
        return connection

    @classmethod
    def redis(cls) -> aioredis.Redis:
        return redis

    @classmethod
    def table_name(cls) -> str:
        return "users"

    @classmethod
    def model(cls) -> typing.Type[T]:
        pass

    @classmethod
    def soft_deletes(cls) -> bool:
        pass

    @classmethod
    def default_relations(cls) -> typing.List[str]:
        pass


params = Parameters()
query = UserRepo.select_where(UserRepo.field("name") == params.make("Mojataba"))
print(UserRepo.first(query, params))
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