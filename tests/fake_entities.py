import json
from typing import Type, List, Optional, Dict, Any

from pypika import Field, Query, Column, Table

try:
    from aioredis import Redis
except Exception as ex:
    from redis.asyncio import Redis

from basalam.backbone_orm.migration_abstract import MigrationAbstract
from basalam.backbone_orm.model_abstract import ModelAbstract, T
from basalam.backbone_orm import ModelSchemaAbstract
from basalam.backbone_orm.postgres_connection import PostgresConnection
from basalam.backbone_orm.query_builder_abstract import BaseQueryBuilder
from basalam.backbone_orm.repository_abstract import RepositoryAbstract
from .connections import in_memory_redis_connection, postgres


class FakeAuthor(ModelAbstract):

    def repository(self) -> Any:
        return FakeAuthorRepo

    id: int
    name: str
    metadata: Optional[Dict]

    @property
    def posts(self) -> List["FakePost"]:
        return self.relation("posts")

    @property
    def active_posts(self) -> List["FakePost"]:
        return self.relation("active_posts")

    @property
    def pinned_post(self) -> Optional["FakePost"]:
        return self.relation("pinned_post")


class FakeAuthorSchema(ModelSchemaAbstract):
    ID = "id"
    NAME = "name"
    METADATA = "metadata"


def add_jr(x):
    return x + " jr"


def add_dot(x):
    return x + "."


class FakeAuthorRepo(RepositoryAbstract[FakeAuthor, BaseQueryBuilder]):

    @classmethod
    def schema_name(cls) -> str:
        return 'public'

    @classmethod
    async def connection(cls) -> PostgresConnection:
        return await postgres.acquire()

    @classmethod
    async def redis(cls) -> Redis:
        return in_memory_redis_connection

    @classmethod
    def accessors(cls):
        return {
            FakeAuthorSchema.NAME: [add_jr, add_dot],
            FakeAuthorSchema.METADATA: lambda property_: json.loads(property_)
            if property_ is not None
            else None,
        }

    @classmethod
    def mutators(cls):
        return {
            FakeAuthorSchema.METADATA: lambda property_: json.dumps(property_)
            if property_ is not None
            else None
        }

    @classmethod
    def schema(cls) -> Type[ModelSchemaAbstract]:
        return FakeAuthorSchema

    @classmethod
    def table_name(cls) -> str:
        return "fake_authors"

    @classmethod
    def model(cls) -> Type[T]:
        return FakeAuthor

    @classmethod
    def soft_deletes(cls) -> bool:
        return False

    @classmethod
    def default_relations(cls) -> List[str]:
        return []

    @classmethod
    def posts_relation(cls):
        return cls.has_many(FakePostRepo, "author_id", "id")

    @classmethod
    def active_posts_relation(cls):
        return cls.has_many(FakePostRepo, "author_id", "id").callback(
            lambda query: query.where(Field("is_active") == 1)
        )

    @classmethod
    def pinned_post_relation(cls):
        return cls.belongs_to(FakePostRepo, "metadata.nested.pinned_post_id", "id")

    @classmethod
    def created_at_field(cls) -> Optional[str]:
        return None

    @classmethod
    def updated_at_field(cls) -> Optional[str]:
        return None


# -----------------


class FakePost(ModelAbstract):

    def repository(self) -> Any:
        return FakePostRepo

    id: int
    title: str
    author_id: int
    is_active: int

    @property
    def author(self) -> Optional["FakeAuthor"]:
        return self.relation("author")

    @property
    def tags(self) -> List["FakeTag"]:
        return self.relation("tags")

    @property
    def comments(self) -> List["FakeComment"]:
        return self.relation("comments")


class FakePostRepo(RepositoryAbstract[FakePost, BaseQueryBuilder]):

    @classmethod
    def schema_name(cls) -> str:
        return 'public'

    @classmethod
    async def connection(cls) -> PostgresConnection:
        return await postgres.acquire()

    @classmethod
    async def redis(cls) -> Redis:
        return in_memory_redis_connection

    @classmethod
    def table_name(cls) -> str:
        return "fake_posts"

    @classmethod
    def model(cls) -> Type[T]:
        return FakePost

    @classmethod
    def soft_deletes(cls) -> bool:
        return False

    @classmethod
    def default_relations(cls) -> List[str]:
        return []

    @classmethod
    def author_relation(cls):
        return cls.belongs_to(FakeAuthorRepo, "author_id", "id")

    @classmethod
    def tags_relation(cls):
        return cls.belongs_to_many(
            FakeTagRepo, "fake_posts_to_fake_tags", "id", "post_id", "id", "tag_id"
        )

    @classmethod
    def comments_relation(cls):
        return cls.belongs_to_many(
            FakeCommentRepo,
            "fake_posts_to_fake_comments",
            "id",
            "post_id",
            "id",
            "comment_id",
        )

    @classmethod
    def created_at_field(cls) -> Optional[str]:
        return None

    @classmethod
    def updated_at_field(cls) -> Optional[str]:
        return None


# -----------------


class FakeTag(ModelAbstract):

    def repository(self) -> Any:
        return FakeTagRepo

    id: int
    title: str

    @property
    def posts(self) -> List[FakePost]:
        return self.relation("posts")


class FakeTagRepo(RepositoryAbstract[FakeTag, BaseQueryBuilder]):

    @classmethod
    def schema_name(cls) -> str:
        return 'public'

    @classmethod
    async def connection(cls) -> PostgresConnection:
        return await postgres.acquire()

    @classmethod
    async def redis(cls) -> Redis:
        return in_memory_redis_connection

    @classmethod
    def table_name(cls) -> str:
        return "fake_tags"

    @classmethod
    def model(cls) -> Type[T]:
        return FakeTag

    @classmethod
    def soft_deletes(cls) -> bool:
        return False

    @classmethod
    def default_relations(cls) -> List[str]:
        return []

    @classmethod
    def posts_relation(cls):
        return cls.belongs_to_many(
            FakePostRepo, "fake_posts_to_fake_tags", "id", "tag_id", "id", "post_id"
        )

    @classmethod
    def created_at_field(cls) -> Optional[str]:
        return None

    @classmethod
    def updated_at_field(cls) -> Optional[str]:
        return None


# -----------------


class FakeComment(ModelAbstract):

    def repository(self) -> Any:
        return FakeCommentRepo

    id: int
    comment: str

    @property
    def posts(self) -> List[FakePost]:
        return self.relation("posts")


class FakeCommentRepo(RepositoryAbstract[FakeComment, BaseQueryBuilder]):

    @classmethod
    def schema_name(cls) -> str:
        return 'public'

    @classmethod
    async def connection(cls) -> PostgresConnection:
        return await postgres.acquire()

    @classmethod
    async def redis(cls) -> Redis:
        return in_memory_redis_connection

    @classmethod
    def table_name(cls) -> str:
        return "fake_comments"

    @classmethod
    def model(cls) -> Type[T]:
        return FakeComment

    @classmethod
    def soft_deletes(cls) -> bool:
        return False

    @classmethod
    def default_relations(cls) -> List[str]:
        return []

    @classmethod
    def posts_relation(cls):
        return cls.belongs_to_many(
            FakePostRepo,
            "fake_posts_to_fake_comments",
            "id",
            "comment_id",
            "id",
            "post_id",
        )

    @classmethod
    def created_at_field(cls) -> Optional[str]:
        return None

    @classmethod
    def updated_at_field(cls) -> Optional[str]:
        return None


# -----------------


class FakePostToTag(ModelAbstract):

    def repository(self) -> Any:
        return FakePostToTagRepo

    post_id: int
    tag_id: int


class FakePostToTagRepo(RepositoryAbstract[FakePostToTag, BaseQueryBuilder]):

    @classmethod
    def schema_name(cls) -> str:
        return 'public'

    @classmethod
    async def connection(cls) -> PostgresConnection:
        return await postgres.acquire()

    @classmethod
    async def redis(cls) -> Redis:
        return in_memory_redis_connection

    @classmethod
    def table_name(cls) -> str:
        return "fake_posts_to_fake_tags"

    @classmethod
    def model(cls) -> Type[T]:
        return FakePostToTag

    @classmethod
    def soft_deletes(cls) -> bool:
        return False

    @classmethod
    def default_relations(cls) -> List[str]:
        return []

    @classmethod
    def created_at_field(cls) -> Optional[str]:
        return None

    @classmethod
    def updated_at_field(cls) -> Optional[str]:
        return None


# -----------------


class FakePostToComment(ModelAbstract):

    def repository(self) -> Any:
        return FakePostToCommentRepo

    post_id: int
    comment_id: int


class FakePostToCommentRepo(RepositoryAbstract[FakePostToComment, BaseQueryBuilder]):

    @classmethod
    def schema_name(cls) -> str:
        return 'public'

    @classmethod
    async def connection(cls) -> PostgresConnection:
        return await postgres.acquire()

    @classmethod
    async def redis(cls) -> Redis:
        return in_memory_redis_connection

    @classmethod
    def table_name(cls) -> str:
        return "fake_posts_to_fake_comments"

    @classmethod
    def model(cls) -> Type[T]:
        return FakePostToComment

    @classmethod
    def soft_deletes(cls) -> bool:
        return False

    @classmethod
    def default_relations(cls) -> List[str]:
        return []

    @classmethod
    def created_at_field(cls) -> Optional[str]:
        return None

    @classmethod
    def updated_at_field(cls) -> Optional[str]:
        return None


class MigrateFakeEntities(MigrationAbstract):

    async def setup(self):
        await (await postgres.acquire()).execute(
            Query.create_table(Table("fake_authors", schema='public'))
            .columns(
                Column("id", "SERIAL", nullable=False),
                Column("name", "VARCHAR(100)"),
                Column("metadata", "jsonb", nullable=True),
            )
            .primary_key("id")
            .get_sql()
        )

        await (await postgres.acquire()).execute(
            Query.create_table(Table("fake_posts", schema='public'))
            .columns(
                Column("id", "SERIAL", nullable=False),
                Column("author_id", "INT"),
                Column("is_active", "INT DEFAULT(1)"),
                Column("title", "VARCHAR(100)"),
            )
            .primary_key("id")
            .get_sql()
        )

        await (await postgres.acquire()).execute(
            Query.create_table(Table("fake_tags", schema='public'))
            .columns(
                Column("id", "SERIAL", nullable=False), Column("title", "VARCHAR(100)")
            )
            .primary_key("id")
            .get_sql()
        )

        await (await postgres.acquire()).execute(
            Query.create_table(Table("fake_comments", schema='public'))
            .columns(
                Column("id", "SERIAL", nullable=False),
                Column("comment", "VARCHAR(500)"),
            )
            .primary_key("id")
            .get_sql()
        )

        await (await postgres.acquire()).execute(
            Query.create_table(Table("fake_posts_to_fake_tags", schema='public'))
            .columns(Column("tag_id", "INT"), Column("post_id", "INT"))
            .get_sql()
        )

        await (await postgres.acquire()).execute(
            Query.create_table(Table("fake_posts_to_fake_comments", schema='public'))
            .columns(Column("comment_id", "INT"), Column("post_id", "INT"))
            .get_sql()
        )

    async def teardown(self):
        (await postgres.acquire()).allow_wildcard_queries()
        await (await postgres.acquire()).execute("DROP TABLE fake_authors")
        await (await postgres.acquire()).execute("DROP TABLE fake_posts")
        await (await postgres.acquire()).execute("DROP TABLE fake_tags")
        await (await postgres.acquire()).execute("DROP TABLE fake_comments")
        await (await postgres.acquire()).execute(
            "DROP TABLE fake_posts_to_fake_tags"
        )
        await (await postgres.acquire()).execute(
            "DROP TABLE fake_posts_to_fake_comments"
        )
        (await postgres.acquire()).deny_wildcard_queries()
