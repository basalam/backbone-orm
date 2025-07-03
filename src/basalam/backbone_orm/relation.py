from abc import ABC, abstractmethod
from typing import List, TYPE_CHECKING, Type, Optional, Callable, Any, Dict

from pypika import Table, Field
from pypika.queries import QueryBuilder

from .parameters import Parameters

if TYPE_CHECKING:
    from basalam.backbone_orm.model_abstract import ModelAbstract
    from basalam.backbone_orm.repository_abstract import RepositoryAbstract


class Relation(ABC):

    def __init__(self):
        self.relation_repo: Optional[RepositoryAbstract] = None
        self.query_callback: Optional[
            Callable[[QueryBuilder], QueryBuilder]
        ] = lambda query: query
        self.attribute_getter: Optional[
            Callable[["ModelAbstract", "str"], Any]
        ] = self.default_attribute_getter

    def default_attribute_getter(self, model: "ModelAbstract", key: str):
        parts: List[str] = key.split(".")
        if len(parts) == 1:
            return model.__getattribute__(key)
        else:
            value = model

            for part in parts:
                if isinstance(value, Dict):
                    value = value.get(part)
                else:
                    value = value.__getattribute__(part)

                if value is None:
                    return None

            return value

    def callback(
            self, callback: Optional[Callable[[QueryBuilder], QueryBuilder]]
    ) -> "Relation":
        self.query_callback = callback
        return self

    def with_attribute_getter(self, getter: Callable):
        self.attribute_getter = getter
        return self

    async def apply(self, key: str, model: "ModelAbstract") -> "ModelAbstract":
        return (await self.apply_many(key, [model]))[0]

    def compare(self, reference_prop, relation_prop):
        if isinstance(reference_prop, int) and isinstance(relation_prop, str):
            return reference_prop == int(relation_prop)
        elif isinstance(relation_prop, int) and isinstance(reference_prop, str):
            return int(reference_prop) == relation_prop
        else:
            return reference_prop == relation_prop

    @abstractmethod
    async def apply_many(
            self, key: str, models: List["ModelAbstract"]
    ) -> List["ModelAbstract"]:
        pass

    @abstractmethod
    async def forget(self, model: "ModelAbstract", relation_key: str):
        pass


class BelongsTo(Relation):

    def __init__(
            self,
            local_repo: Type["RepositoryAbstract"],
            relation_repo: Type["RepositoryAbstract"],
            foreign_key: Optional[str] = None,
            local_key: Optional[str] = None,
            with_trashed: bool = False,
            cache_time_in_seconds=0,
    ):
        super().__init__()

        if local_key is None:
            local_key = relation_repo.identifier()
        if foreign_key is None:
            foreign_key = relation_repo.foreign_key_identifier()

        self.with_trashed = with_trashed
        self.foreign_key = foreign_key
        self.local_key = local_key
        self.local_repo = local_repo
        self.relation_repo = relation_repo
        self.cache_time_in_seconds = cache_time_in_seconds

    async def apply_many(
            self, relation_name: str, models: List["ModelAbstract"]
    ) -> List["ModelAbstract"]:
        if len(models) == 0:
            return models

        for model in models:
            model.forget_relation(relation_name)

        identifiers = await self.identifiers(relation_name, models)

        if len(identifiers) == 0:
            results = []
        else:
            params = Parameters()
            query = self.relation_repo.select_query(self.with_trashed)
            query = query.where(
                Field(self.local_key).isin(params.make_many(identifiers))
            )
            query = query.select("*")
            query = self.query_callback(query)
            results = await self.relation_repo.get(query, params=params)

        new_caches = {}
        for model in models:
            compare_fn = lambda result: self.compare(
                self.attribute_getter(model, self.foreign_key),
                result.__getattribute__(self.local_key),
            )
            matches = [result for result in results if compare_fn(result)]

            if len(matches) > 0:
                model.set_relation(relation_name, matches[0])

            if self.cache_time_in_seconds > 0 and len(matches) > 0:
                new_caches[self.cache_key(model, relation_name)] = matches[0]

        if len(new_caches.keys()) > 0:
            await (await self.local_repo.cache()).mset(
                new_caches, self.cache_time_in_seconds
            )

        return models

    async def identifiers(self, relation_name: str, models: List["ModelAbstract"]):
        if self.cache_time_in_seconds > 0:
            identifiers = []
            cache_keys = [self.cache_key(model, relation_name) for model in models]
            caches = await (await self.local_repo.cache()).mget(cache_keys)
            for index, model in enumerate(models):
                if caches[index] is not None:
                    model.set_relation(relation_name, caches[index])
                else:
                    identifiers.append(self.attribute_getter(model, self.foreign_key))
        else:
            identifiers = [
                self.attribute_getter(model, self.foreign_key) for model in models
            ]

        identifiers = {
            identifier for identifier in identifiers if identifier is not None
        }
        return identifiers

    def cache_key(self, model: "ModelAbstract", relation_key: str):
        table = self.local_repo.table_name()
        identifier = model.__getattribute__(self.local_repo.identifier())
        # timestamp = model.__getattribute__(self.local_repo.updated_at_field()) if self.local_repo.updated_at_field() is not None else 0
        return f"relation::belongs_to::{table}::{identifier}::{relation_key}"

    async def forget(self, model: "ModelAbstract", relation_key: str):
        await (await self.local_repo.cache()).forget(
            self.cache_key(model, relation_key)
        )


class HasOne(Relation):

    async def forget(self, model: "ModelAbstract", relation_key: str):
        await BelongsTo(
            self.local_repo,
            self.relation_repo,
            self.local_key,
            self.foreign_key,
            self.with_trashed,
            self.cache_time_in_seconds,
        ).forget(model, relation_key)

    def __init__(
            self,
            local_repo: Type["RepositoryAbstract"],
            relation_repo: Type["RepositoryAbstract"],
            foreign_key: Optional[str] = None,
            local_key: Optional[str] = None,
            with_trashed: bool = False,
            cache_time_in_seconds: int = 0,
    ):
        super().__init__()

        if local_key is None:
            local_key = local_repo.identifier()
        if foreign_key is None:
            foreign_key = local_repo.foreign_key_identifier()

        self.with_trashed = with_trashed
        self.foreign_key = foreign_key
        self.local_key = local_key
        self.local_repo = local_repo
        self.relation_repo = relation_repo
        self.cache_time_in_seconds = cache_time_in_seconds

    async def apply_many(
            self, relation_name: str, models: List["ModelAbstract"]
    ) -> List["ModelAbstract"]:
        return await BelongsTo(
            self.local_repo,
            self.relation_repo,
            self.local_key,
            self.foreign_key,
            self.with_trashed,
            self.cache_time_in_seconds,
        ).apply_many(relation_name, models)


class HasMany(Relation):

    def __init__(
            self,
            local_repo: Type["RepositoryAbstract"],
            relation_repo: Type["RepositoryAbstract"],
            foreign_key: Optional[str] = None,
            local_key: Optional[str] = None,
            with_trashed: bool = False,
            cache_time_in_seconds=0
    ):
        super().__init__()

        if local_key is None:
            local_key = local_repo.identifier()
        if foreign_key is None:
            foreign_key = local_repo.foreign_key_identifier()

        self.with_trashed = with_trashed
        self.foreign_key = foreign_key
        self.local_key = local_key
        self.local_repo = local_repo
        self.relation_repo = relation_repo
        self.cache_time_in_seconds = cache_time_in_seconds

    async def apply_many(
            self, relation_name: str, models: List["ModelAbstract"]
    ) -> List["ModelAbstract"]:
        if len(models) == 0:
            return models

        for model in models:
            model.forget_relation(relation_name)

        identifiers = await self.identifiers(relation_name, models)

        if len(identifiers) == 0:
            results = []
        else:
            params = Parameters()
            query = self.relation_repo.select_query(self.with_trashed)
            query = query.where(
                Field(self.foreign_key).isin(params.make_many(identifiers))
            )
            query = query.select("*")
            query = self.query_callback(query)
            results = await self.relation_repo.get(query, params)

        new_caches = {}
        for model in models:
            matches = [
                result
                for result in results
                if self.compare(
                    self.attribute_getter(model, self.local_key),
                    self.attribute_getter(result, self.foreign_key),
                )
            ]

            if len(matches) > 0:
                model.set_relation(relation_name, matches)
            elif model.__getattribute__(self.local_repo.identifier()) in identifiers:
                model.set_relation(relation_name, [])

            if self.cache_time_in_seconds > 0 and len(matches) > 0:
                new_caches[self.cache_key(model, relation_name)] = matches

        if len(new_caches.keys()) > 0:
            await (await self.local_repo.cache()).mset(
                new_caches, self.cache_time_in_seconds
            )

        return models

    async def identifiers(self, relation_name: str, models: List["ModelAbstract"]):
        if self.cache_time_in_seconds > 0:
            identifiers = []
            cache_keys = [self.cache_key(model, relation_name) for model in models]
            caches = await (await self.local_repo.cache()).mget(cache_keys)
            for index, model in enumerate(models):
                if caches[index] is not None:
                    model.set_relation(relation_name, caches[index])
                else:
                    identifiers.append(self.attribute_getter(model, self.local_key))
        else:
            identifiers = [
                self.attribute_getter(model, self.local_key) for model in models
            ]

        identifiers = {
            identifier for identifier in identifiers if identifier is not None
        }
        return identifiers

    def cache_key(self, model: "ModelAbstract", relation_key: str):
        table = self.local_repo.table_name()
        identifier = model.__getattribute__(self.local_repo.identifier())
        return f"relation::has_many::{table}::{identifier}::{relation_key}"

    async def forget(self, model: "ModelAbstract", relation_key: str):
        await (await self.local_repo.cache()).forget(
            self.cache_key(model, relation_key)
        )


class BelongsToMany(Relation):

    def __init__(
            self,
            local_repo: Type["RepositoryAbstract"],
            relation_repo: Type["RepositoryAbstract"],
            pivot_table: str,
            pivot_schema: Optional[str] = None,
            local_key: Optional[str] = None,
            pivot_local_key: Optional[str] = None,
            relation_key: Optional[str] = None,
            pivot_relation_key: Optional[str] = None,
            with_trashed: bool = False,
            cache_time_in_seconds: int = 0,
    ):
        super().__init__()

        if local_key is None:
            local_key = local_repo.identifier()
        if relation_key is None:
            relation_key = relation_repo.identifier()
        if pivot_local_key is None:
            pivot_local_key = local_repo.foreign_key_identifier()
        if pivot_relation_key is None:
            pivot_relation_key = relation_repo.foreign_key_identifier()

        self.with_trashed = with_trashed
        self.pivot_table = pivot_table
        self.pivot_schema = pivot_schema
        self.local_key = local_key
        self.pivot_local_key = pivot_local_key
        self.relation_key = relation_key
        self.pivot_relation_key = pivot_relation_key
        self.local_repo = local_repo
        self.relation_repo = relation_repo
        self.cache_time_in_seconds = cache_time_in_seconds

    async def apply_many(
            self, relation_name: str, models: List["ModelAbstract"]
    ) -> List["ModelAbstract"]:
        if len(models) == 0:
            return models

        for model in models:
            model.forget_relation(relation_name)

        identifiers = await self.identifiers(relation_name, models)

        if len(identifiers) == 0:
            results = []
        else:
            params = Parameters()
            pivot_table = Table(self.pivot_table, schema=self.pivot_schema if self.pivot_schema else self.relation_repo.schema_name())
            relation_table = self.relation_repo.table()
            query = self.relation_repo.select_query(self.with_trashed)
            query = query.inner_join(pivot_table).on(
                pivot_table.field(self.pivot_relation_key) == relation_table.field(self.relation_key)
            )
            query = self.query_callback(query)
            query = query.where(
                pivot_table.field(self.pivot_local_key).isin(
                    params.make_many(identifiers)
                )
            )
            query = query.select(
                relation_table.star,
                pivot_table.field(self.pivot_local_key).as_("x_ref"),
            )
            results = await self.relation_repo.get(query, params=params)

        new_caches = {}

        non_cache_models = [
            model
            for model in models
            if model.__getattribute__(self.local_repo.identifier()) in identifiers
        ]

        for model in non_cache_models:
            model.x_relations[relation_name] = list()
            for result in results:
                if self.compare(model.__getattribute__(self.local_key), result.x_ref):
                    model.x_relations[relation_name].append(result)

            if (
                    self.cache_time_in_seconds > 0
                    and len(model.x_relations[relation_name]) > 0
            ):
                new_caches[self.cache_key(model, relation_name)] = model.x_relations[
                    relation_name
                ]

        if len(new_caches.keys()) > 0:
            await (await self.local_repo.cache()).mset(
                new_caches, self.cache_time_in_seconds
            )

        return models

    async def identifiers(self, relation_key: str, models: List["ModelAbstract"]):
        if self.cache_time_in_seconds > 0:
            identifiers = []
            cache_keys = [self.cache_key(model, relation_key) for model in models]
            caches = await (await self.local_repo.cache()).mget(cache_keys)
            for index, model in enumerate(models):
                if caches[index] is not None:
                    model.set_relation(relation_key, caches[index])
                else:
                    identifiers.append(self.attribute_getter(model, self.local_key))

        else:
            identifiers = [
                self.attribute_getter(model, self.local_key) for model in models
            ]

        identifiers = {
            identifier for identifier in identifiers if identifier is not None
        }

        return identifiers

    def cache_key(self, model: "ModelAbstract", relation_key: str):
        table = self.local_repo.table_name()
        identifier = model.__getattribute__(self.local_repo.identifier())
        # timestamp = model.__getattribute__(self.local_repo.updated_at_field()) if self.local_repo.updated_at_field() is not None else 0
        return f"relation::belongs_to_many::{table}::{identifier}::{relation_key}"

    async def forget(self, model: "ModelAbstract", relation_key: str):
        await (await self.local_repo.cache()).forget(
            self.cache_key(model, relation_key)
        )
