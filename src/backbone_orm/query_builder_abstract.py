from abc import ABC
from datetime import datetime
from typing import Type, TYPE_CHECKING, Union, List, TypeVar

from pypika.dialects import PostgreSQLQueryBuilder

if TYPE_CHECKING:
    from . import RepositoryAbstract


class QueryBuilderAbstract(PostgreSQLQueryBuilder, ABC):

    def __init__(self, *args, repo: Type["RepositoryAbstract"] = None, **kwargs, ):
        super().__init__(*args, **kwargs)
        self.repo: Type["RepositoryAbstract"] = repo

    @classmethod
    def from_repo(cls, repo: Type["RepositoryAbstract"]):
        return cls(repo=repo).from_(repo.table())

    def filter_with_trashed(self):
        return self.where(
            self.repo.field(self.repo.soft_delete_identifier()).isnull()
        )

    def ensure_list(self, value) -> List:
        if isinstance(value, list):
            return value
        return [value]

    def filter_id(self, id: Union[int, List[int]]):
        if id is None: return self
        return self.where(self.repo.field(self.repo.identifier()).isin(self.ensure_list(id)))

    def filter_min_created_at(self, timestamp: int):
        if timestamp is None: return self
        return self.where(
            self.repo.field(self.repo.created_at_field()).gte(datetime.fromtimestamp(timestamp))
        )

    def filter_max_created_at(self, timestamp: int):
        if timestamp is None: return self
        return self.where(
            self.repo.field(self.repo.created_at_field()).lte(datetime.fromtimestamp(timestamp))
        )

    def select_star(self):
        return self.select(self.repo.table().star)


V = TypeVar("V", bound=QueryBuilderAbstract)


class BaseQueryBuilder(QueryBuilderAbstract):
    pass
