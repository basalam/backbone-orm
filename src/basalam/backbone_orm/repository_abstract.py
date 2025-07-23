import datetime
import pickle
from abc import ABC, abstractmethod
from typing import Dict, List, Type, Union, Generic, Optional, Any, Callable, Iterable

try:
    from aioredis import Redis
except Exception as ex:
    from redis.asyncio import Redis

import inflect
from basalam.backbone_redis_cache import RedisCache
from pypika import Table, Field, functions
from pypika.queries import QueryBuilder

from .model_schema_abstract import ModelSchemaAbstract
from .parameters import Parameters
from .postgres_connection import PostgresConnection
from .relation_applier import RelationApplier
from .query_builder_abstract import QueryBuilderAbstract, V
from .model_abstract import T
from .relation import Relation, BelongsTo, HasOne, HasMany, BelongsToMany


class RepositoryAbstract(ABC, Generic[T, V]):

    @classmethod
    @abstractmethod
    async def connection(cls) -> PostgresConnection:
        pass

    @classmethod
    @abstractmethod
    async def redis(cls) -> Redis:
        pass

    @classmethod
    def query_builder(cls) -> QueryBuilderAbstract:
        return QueryBuilderAbstract()

    @classmethod
    @abstractmethod
    def schema_name(cls) -> str:
        pass

    @classmethod
    async def cache(cls) -> RedisCache:
        return RedisCache(
            connection=await cls.redis(),
            prefix="BACKBONE_ORM.CACHE." + cls.table_name() + ".",
            serializer=pickle.dumps,
            deserializer=pickle.loads,
        )

    @classmethod
    @abstractmethod
    def table_name(cls) -> str:
        pass

    @classmethod
    @abstractmethod
    def model(cls) -> Type[T]:
        pass

    @classmethod
    def schema(cls) -> Type[ModelSchemaAbstract]:
        pass

    @classmethod
    @abstractmethod
    def soft_deletes(cls) -> bool:
        pass

    @classmethod
    @abstractmethod
    def default_relations(cls) -> List[str]:
        pass

    @classmethod
    def accessors(cls) -> Dict[str, Union[Callable, List[Callable]]]:
        return {}

    @classmethod
    def mutators(cls) -> Dict[str, Union[Callable, List[Callable]]]:
        return {}

    @classmethod
    def updated_at_field(cls) -> Optional[str]:
        return "updated_at"

    @classmethod
    def created_at_field(cls) -> Optional[str]:
        return "created_at"

    @classmethod
    async def apply_default_relations(cls, models: List[T]) -> List[T]:
        return await cls.apply_relations(models, cls.default_relations())

    @classmethod
    def identifier(cls) -> str:
        return "id"

    @classmethod
    def foreign_key_identifier(cls):
        return inflect.engine().singular_noun(cls.table_name()) + "_" + cls.identifier()

    @classmethod
    def soft_delete_identifier(cls) -> str:
        return "deleted_at"

    @classmethod
    def cast_to_models(cls, rows: List[Dict]) -> List[T]:
        model_class = cls.model()
        return [model_class(**row, x_original=dict(**row)) for row in rows]

    @classmethod
    def cast_to_model(cls, row: Dict) -> T:
        return cls.cast_to_models([row])[0]

    @classmethod
    def expand_relations(
            cls, models: Union[T, List[T]], relations: List[str]
    ) -> List[str]:
        first_model = models[0] if isinstance(models, list) else models
        new_relations = []
        for relation in relations:
            if relation[-1:] == "*":
                matching_relations = [
                    rel
                    for rel in first_model.x_applied_relations
                    if rel.startswith(relation[:-1])
                ]
                for item in matching_relations:
                    new_relations.append(item)
            else:
                new_relations.append(relation)

        return list(set(new_relations))

    @classmethod
    async def reapply_relations(
            cls, models: Union[T, List[T]], relations: List[str]
    ) -> Union[T, List[T]]:
        for relation in cls.expand_relations(models, relations):
            await cls.reapply_relation(models, relation)

        return models

    @classmethod
    def forget_relations(
            cls, models: Union[T, List[T]], relations: List[str]
    ) -> Union[T, List[T]]:
        for relation in cls.expand_relations(models, relations):
            cls.forget_relation(models, relation)

        return models

    @classmethod
    async def reapply_relation(
            cls, models: Union[T, List[T]], relation: str
    ) -> Union[T, List[T]]:
        cls.forget_relation(models, relation)
        return await cls.apply_relation(models, relation)

    @classmethod
    def forget_relation(cls, models: Union[T, List[T]], relation: str) -> None:
        models: List[T] = models if isinstance(models, list) else [models]
        for model in models:
            main_relation, _, nested_relation = relation.partition(".")
            model.x_applied_relations = set(
                [
                    item
                    for item in model.x_applied_relations
                    if item not in [main_relation, relation]
                ]
            )

    @classmethod
    async def apply_relations(
            cls, models: Union[T, List[T]], relations: Iterable[str]
    ) -> Union[T, List[T]]:
        for relation in relations:
            models = await cls.apply_relation(models, relation)

        return models

    @classmethod
    async def apply_cached_relations(
            cls, models: List[T], relations: List[str], cache_time_in_seconds: int
    ) -> Union[T, List[T]]:
        non_cached_models = []

        cache_key_fn = (
            lambda
            model: f"relations::{cls.table_name()}::{model.__getattribute__(cls.identifier())}::{'__'.join(relations)}"
        )
        caches = await (await cls.cache()).mget(
            [cache_key_fn(model) for model in models]
        )
        for index, model in enumerate(models):
            cached: Optional[Dict] = caches[index]
            if cached is not None:
                for key, value in cached.items():
                    model.set_relation(key, value)
            else:
                non_cached_models.append(model)

        await cls.apply_relations(non_cached_models, relations)

        top_level_relations = set([relation.split(".")[0]
                                  for relation in relations])

        new_caches = {
            cache_key_fn(model): {
                relation: model.__getattribute__(relation)
                for relation in top_level_relations
            }
            for model in non_cached_models
        }

        await (await cls.cache()).mset(new_caches, cache_time_in_seconds)

        return models

    @classmethod
    async def apply_relation(
            cls, models: Union[T, List[T]], relation: str
    ) -> Union[T, List[T]]:
        return await RelationApplier(cls, models, relation).apply()

    @classmethod
    async def execute(
            cls,
            query: Union[QueryBuilder, str],
            params: Optional[Parameters] = None,
            return_: bool = False,
    ):
        if params is None:
            params = Parameters()

        query_str = query if type(query) is str else query.get_sql()
        if return_:
            return await (await cls.connection()).execute_and_fetch(
                query_str, params.values()
            )
        else:
            return await (await cls.connection()).execute(query_str, params.values())

    @classmethod
    async def execute_and_fetch(
            cls, query: Union[QueryBuilder, str], params: Optional[Parameters] = None
    ):
        return await cls.execute(query, params, return_=True)

    @classmethod
    def table(cls) -> Table:
        return Table(cls.table_name(), schema=cls.schema_name())

    @classmethod
    def query(cls) -> Table:
        return cls.table()

    @classmethod
    def field(cls, name: str) -> Field:
        return cls.query().field(name)

    @classmethod
    def select_query(cls, with_thrashed: bool = False) -> V:
        query = cls.query_builder().from_repo(cls)

        if cls.soft_deletes() and with_thrashed is False:
            query = query.filter_with_trashed()

        return query

    @classmethod
    def delete_query(cls) -> QueryBuilder:
        return cls.select_query().delete()

    @classmethod
    def insert_query(cls) -> QueryBuilder:
        return cls.query().insert()

    @classmethod
    def update_query(cls) -> QueryBuilder:
        return cls.query().update()

    @classmethod
    async def create_return(cls, attributes: Dict, **kwargs) -> T:
        return await cls.create({**attributes, **kwargs}, True)

    @classmethod
    async def create(
            cls, attributes: Dict, return_: bool = False, **kwargs
    ) -> Optional[T]:
        attributes = cls.apply_mutators(attributes)
        attributes = {**attributes, **kwargs}

        if (
                cls.created_at_field() is not None
                and cls.created_at_field() not in attributes.keys()
        ):
            attributes[cls.created_at_field()] = datetime.datetime.now().replace(
                microsecond=0
            )

        if (
                cls.updated_at_field() is not None
                and cls.updated_at_field() not in attributes.keys()
        ):
            attributes[cls.updated_at_field()] = datetime.datetime.now().replace(
                microsecond=0
            )

        params = Parameters(*attributes.values())
        query = cls.insert_query().insert(params.bindings()).columns(*attributes.keys())

        if return_:
            items = await cls.execute_and_fetch(
                query.get_sql() + " RETURNING *", params
            )
            return (await cls.normalize(items))[0]
        else:
            return await cls.execute(query, params)

    @classmethod
    async def create_return_many(cls, attributes: List[Dict]) -> List[T]:
        return await cls.create_many(attributes, True)

    @classmethod
    async def create_many(
            cls, attributes: List[Dict], return_: bool = False
    ) -> Optional[List[T]]:
        if len(attributes) == 0:
            return []

        for index, attribute_group in enumerate(attributes):
            attributes[index] = cls.apply_mutators(attribute_group)

        columns = [key for key in attributes[0].keys()]

        if cls.created_at_field() is not None and cls.created_at_field() not in columns:
            columns.append(cls.created_at_field())
            for attribute_group in attributes:
                attribute_group[
                    cls.created_at_field()
                ] = datetime.datetime.now().replace(microsecond=0)

        if cls.updated_at_field() is not None and cls.updated_at_field() not in columns:
            columns.append(cls.updated_at_field())
            for attribute_group in attributes:
                attribute_group[
                    cls.updated_at_field()
                ] = datetime.datetime.now().replace(microsecond=0)

        query = cls.insert_query().columns(*columns)

        params = Parameters()
        for attribute_group in attributes:
            group_params = params.make_many(attribute_group.values())
            query = query.insert(group_params)

        if return_:
            rows = await cls.execute_and_fetch(query.get_sql() + " RETURNING *", params)
            return await cls.normalize(rows)
        else:
            return await cls.execute(query, params)

    @classmethod
    async def update_return(cls, query: QueryBuilder, attributes: Dict) -> Union[T, List[T]]:
        return await cls.update(query, attributes, True)

    @classmethod
    async def update_where_in(cls, field, identifiers: List, attributes: Dict):
        if len(identifiers) == 0:
            return

        params = Parameters(*identifiers)
        return await cls.update(
            cls.update_query().where(cls.field(field).isin(params.bindings())),
            attributes,
            params=params,
        )

    @classmethod
    async def update_where_identifier_in(cls, identifiers: List, attributes: Dict):
        return await cls.update_where_in(cls.identifier(), identifiers, attributes)

    @classmethod
    async def update(
            cls,
            query: QueryBuilder,
            attributes: Dict,
            return_: bool = False,
            params: Optional[Parameters] = None,
    ) -> Optional[Union[T, List[T]]]:
        cls.apply_mutators(attributes)
        params = params if params is not None else Parameters()

        for key, value in attributes.items():
            query = query.set(key, params.make(value))

        if (
                cls.updated_at_field() is not None
                and cls.updated_at_field() not in attributes.keys()
        ):
            query = query.set(
                cls.updated_at_field(),
                params.make(datetime.datetime.now().replace(microsecond=0)),
            )

        if return_:
            return await cls.normalize(
                await cls.execute_and_fetch(query.get_sql() + " RETURNING *", params)
            )
        else:
            return await cls.execute(query, params)

    @classmethod
    async def normalize(cls, rows: List[Dict]) -> List[T]:
        return await cls.apply_default_relations(
            cls.cast_to_models(cls.apply_accessors(rows))
        )

    @classmethod
    async def update_return_by_id(cls, identifier: any, attributes: Dict) -> T:
        return await cls.update_by_id(identifier, attributes, True)

    @classmethod
    async def update_by_id(
            cls, identifier: any, attributes: Dict, return_: bool = False
    ) -> Optional[T]:
        if len(attributes.values()) == 0:
            if return_:
                return await cls.find_by_id(identifier)
            else:
                return None

        params = Parameters()
        result = await cls.update(
            cls.update_query().where(
                Field(cls.identifier()).eq(params.make(identifier))
            ),
            attributes,
            return_,
            params=params,
        )
        if return_:
            return result[0]

    @classmethod
    async def update_model(cls, model: T, columns: List[str]):
        attributes = dict()

        for column in columns:
            if hasattr(model, column) and model.__getattribute__(
                    column
            ) != model.x_original.get(column):
                attributes[column] = model.__getattribute__(column)
                model.x_original[column] = model.__getattribute__(column)

        await cls.update_by_id(model.__getattribute__(cls.identifier()), attributes)
        return model

    @classmethod
    async def soft_delete_by_id(cls, identifier: any):
        if isinstance(identifier, List) and len(identifier) == 0:
            return
        elif isinstance(identifier, List):
            params = Parameters(*identifier)
            condition = cls.field(cls.identifier()).isin(params.bindings())
        else:
            params = Parameters()
            condition = cls.field(cls.identifier()).eq(params.make(identifier))

        return await cls.update(
            cls.update_query().where(condition),
            dict(deleted_at=datetime.datetime.now().replace(microsecond=0)),
            params=params,
        )

    @classmethod
    async def hard_delete_by_id(cls, identifier: any):
        if isinstance(identifier, List) and len(identifier) == 0:
            return
        if isinstance(identifier, List):
            params = Parameters(*identifier)
            condition = cls.field(cls.identifier()).isin(params.bindings())
        else:
            params = Parameters()
            condition = cls.field(cls.identifier()).eq(params.make(identifier))

        query = cls.select_query().delete().where(condition)

        return await cls.execute(query, params=params)

    @classmethod
    async def delete_by_id(cls, identifier: Union[Any, List[Any]]):
        if cls.soft_deletes():
            return await cls.soft_delete_by_id(identifier)

        return await cls.hard_delete_by_id(identifier)

    @classmethod
    def delete_model(cls, model: T):
        return cls.delete_by_id(model.__getattribute__(cls.identifier()))

    @classmethod
    def delete_models(cls, models: List[T]):
        return cls.delete_by_id(
            [model.__getattribute__(cls.identifier()) for model in models]
        )

    @classmethod
    async def get(
            cls,
            query: QueryBuilder,
            params: Optional[Parameters] = None,
            relations: Optional[List] = None,
    ) -> List[T]:
        if params is None:
            params = Parameters()

        results = await cls.normalize(await cls.execute_and_fetch(query, params))

        if relations is not None:
            await cls.apply_relations(results, relations)

        return results

    @classmethod
    async def all(cls) -> List[T]:
        return await cls.get(cls.select_query().select("*"))

    @classmethod
    async def first(
            cls,
            query: QueryBuilder,
            params: Optional[Parameters] = None,
            relations: Optional[List] = None,
    ) -> Union[T, None]:
        if params is None:
            params = Parameters()
        result = await cls.get(query.limit(1), params, relations=relations)
        return None if len(result) == 0 else result[0]

    @classmethod
    async def find_by_id(
        cls,
        identifier: any,
        relations: Optional[List] = None,
        with_thrashed: bool = False
    ) -> Union[T, None]:
        if identifier is None:
            return None

        params = Parameters()
        return await cls.first(
            cls.select_query(with_thrashed=with_thrashed)
            .where(Field(cls.identifier()).eq(params.make(identifier)))
            .select("*"),
            params=params,
            relations=relations,
        )

    @classmethod
    async def fresh(cls, model: T, relations: Optional[List] = None) -> T:
        return await cls.find_by_id(
            model.__getattribute__(cls.identifier()), relations=relations
        )

    @classmethod
    def select_where(cls, *args, **kwargs) -> QueryBuilder:
        return cls.select_query().where(*args, **kwargs)

    @classmethod
    async def exists(cls, query: QueryBuilder) -> bool:
        return await cls.first(query) is not None

    @classmethod
    async def doesnt_exist(cls, query: QueryBuilder) -> bool:
        return await cls.exists(query) is False

    @classmethod
    async def count(
            cls,
            query: QueryBuilder,
            params: Optional[Parameters] = None,
            column: Any = "*",
            dump: bool = False,
    ) -> Union[float, int]:
        if column != "*":
            query = query.select(functions.Count(
                column).distinct().as_("_aggregate_"))
        else:
            query = query.select(functions.Count(column).as_("_aggregate_"))
        if dump:
            print(query.get_sql(), params.values())
        results = await cls.execute_and_fetch(query, params=params)

        return results[0]["_aggregate_"] if len(results) > 0 else 0

    @classmethod
    async def max(
            cls, query: QueryBuilder, column: str, params: Optional[Parameters] = None
    ) -> Union[float, int, None]:
        query = query.select(functions.Max(column).as_("_aggregate_"))

        results = await cls.execute_and_fetch(query, params=params)
        return results[0]["_aggregate_"] if len(results) > 0 else None

    @classmethod
    async def min(
            cls, query: QueryBuilder, column: str, params: Optional[Parameters] = None
    ) -> Union[float, int, None]:
        query = query.select(functions.Min(column).as_("_aggregate_"))

        results = await cls.execute_and_fetch(query, params=params)
        return results[0]["_aggregate_"] if len(results) > 0 else None

    @classmethod
    async def avg(
            cls, query: QueryBuilder, column: str, params: Optional[Parameters] = None
    ) -> Union[float, int, None]:
        query = query.select(functions.Avg(column).as_("_aggregate_"))

        results = await cls.execute_and_fetch(query, params=params)
        return results[0]["_aggregate_"] if len(results) > 0 else None

    @classmethod
    async def sum(
            cls, query: QueryBuilder, column: Field, params: Optional[Parameters] = None
    ) -> Union[float, int]:
        query = query.select(functions.Sum(column).as_("_aggregate_"))

        results = await cls.execute_and_fetch(query, params=params)

        if len(results) == 0:
            return 0
        if results[0]["_aggregate_"] is None:
            return 0
        return results[0]["_aggregate_"]

    @classmethod
    def belongs_to(
            cls,
            repo: Type["RepositoryAbstract"],
            foreign_key: Optional[str] = None,
            local_key: Optional[str] = None,
            with_trashed: bool = False,
            cache_time_in_seconds=0,
    ):
        return BelongsTo(
            cls, repo, foreign_key, local_key, with_trashed, cache_time_in_seconds
        )

    @classmethod
    def has_one(
            cls,
            repo: Type["RepositoryAbstract"],
            foreign_key: Optional[str] = None,
            local_key: Optional[str] = None,
            with_trashed: bool = False,
            cache_time_in_seconds=0,
    ):
        return HasOne(
            cls, repo, foreign_key, local_key, with_trashed, cache_time_in_seconds
        )

    @classmethod
    def has_many(
            cls,
            repo: Type["RepositoryAbstract"],
            foreign_key: Optional[str] = None,
            local_key: Optional[str] = None,
            with_trashed: bool = False,
            cache_time_in_seconds=0,
    ):
        return HasMany(cls, repo, foreign_key, local_key, with_trashed, cache_time_in_seconds)

    @classmethod
    def belongs_to_many(
            cls,
            repo: Type["RepositoryAbstract"],
            pivot_table: str,
            pivot_schema: Optional[str] = None,
            local_key: Optional[str] = None,
            pivot_local_key: Optional[str] = None,
            relation_key: Optional[str] = None,
            pivot_relation_key: Optional[str] = None,
            with_trashed: bool = False,
            cache_time_in_seconds: int = 0,
    ):
        return BelongsToMany(
            cls,
            repo,
            pivot_table,
            pivot_schema,
            local_key,
            pivot_local_key,
            relation_key,
            pivot_relation_key,
            with_trashed,
            cache_time_in_seconds,
        )

    @classmethod
    async def fresh(cls, model: T) -> T:
        fresh = await cls.find_by_id(model.__getattribute__(cls.identifier()))
        await cls.apply_relations(fresh, model.x_applied_relations)
        return fresh

    @classmethod
    def fill_model(cls, model: T, attrs: Dict) -> T:
        for key, value in attrs.items():
            if hasattr(model, key):
                model.__setattr__(key, value)
        return model

    @classmethod
    async def touch(cls, identifier: Any) -> None:
        await cls.update_by_id(
            identifier,
            {cls.updated_at_field(): datetime.datetime.now().replace(microsecond=0)},
        )

    @classmethod
    def apply_accessors(cls, rows: List[Dict]) -> List[Dict]:
        for row in rows:
            for (key, cast_functions) in cls.accessors().items():
                if not isinstance(cast_functions, list):
                    cast_functions = [cast_functions]
                for cast_function in cast_functions:
                    try:
                        row[key] = cast_function(row.get(key))
                    except Exception as e:
                        raise Exception(
                            f"error while casting {key} of entity {cls.table_name()}#{row[cls.identifier()]}. {e.__str__()}"
                        )
        return rows

    @classmethod
    def apply_mutators(cls, obj: Dict) -> Dict:
        for key, value in obj.items():
            if key in cls.mutators().keys():
                cast_functions = cls.mutators().get(key)
                if not isinstance(cast_functions, list):
                    cast_functions = [cast_functions]
                for cast_function in cast_functions:
                    value = cast_function(value)
                obj[key] = value
        return obj

    @classmethod
    async def random(
            cls, k=1, relations: Optional[List] = None, **kwargs
    ) -> Optional[Union[T, List[T]]]:
        query = cls.select_query()
        query = query.limit(k)
        query = query.orderby(functions.AggregateFunction("random"))
        query = query.select("*")

        params = Parameters()
        for key, value in kwargs.items():
            query = query.where(cls.field(key).eq(params.make(value)))

        if k == 1:
            return await cls.first(query, relations=relations, params=params)
        else:
            return await cls.get(query, relations=relations, params=params)

    @classmethod
    async def forget_relation_cache(cls, model: T, relation_name: str) -> None:
        relation: Relation = getattr(cls, relation_name + "_relation")()
        await relation.forget(model, relation_name)

    @classmethod
    def filter(cls, **kwargs) -> QueryBuilder:
        query: QueryBuilder = cls.select_query().filter_query(**kwargs)
        return query.select_star()
