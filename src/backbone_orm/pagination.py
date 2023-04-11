from abc import ABC, abstractmethod
from typing import List, Type, Callable, Dict, Optional

from backbone_orm import RepositoryAbstract, Parameters
from pydantic import BaseModel
from pypika.queries import QueryBuilder


class PaginationResponse(BaseModel, ABC):
    data: List
    total: int
    per_page: int
    current_page: int
    last_page: int
    from_: int
    to: int

    class Config:
        allow_population_by_field_name = True
        fields = {'from_': 'from'}

    @classmethod
    @abstractmethod
    def repo(cls) -> Type[RepositoryAbstract]: pass

    @classmethod
    @abstractmethod
    def mapper(cls) -> Callable: pass

    @classmethod
    def relations(cls) -> List[str]: pass

    @classmethod
    async def make(
            cls,
            query: QueryBuilder,
            page: int,
            per_page: int,
            params: Optional[Parameters] = None,
            append: Dict = None,
    ) -> "PaginationResponse":
        main_query = query.__copy__().limit(per_page).offset((page - 1) * per_page)
        count_query = query.__copy__()
        entities = await (cls.repo()).get(query=main_query, params=params, relations=cls.relations())

        count_query._selects = []
        count_query._orderbys = []
        total = await (cls.repo()).count(query=count_query, params=params)

        return cls(
            data=[await (cls.mapper())(entity) for entity in entities],
            total=total,
            per_page=per_page,
            current_page=page,
            last_page=int(total / per_page) + 1,
            from_=((page - 1) * per_page) + 1,
            to=((page - 1) * per_page) + len(entities),
            **(append or {})
        )

    @classmethod
    async def resource(
            cls,
            query: QueryBuilder,
            page: int,
            per_page: int,
            params: Optional[Parameters] = None,
            append: Dict = None,
    ) -> Dict:
        return (await cls.make(
            query=query,
            page=page,
            per_page=per_page,
            params=params,
            append=append,
        )).dict(by_alias=True)
