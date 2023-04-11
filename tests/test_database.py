import pytest

from .connections import postgres
from .fake_entities import (
    MigrateFakeEntities,
    FakeAuthorRepo,
    FakePostRepo,
    FakeTagRepo,
    FakePostToTagRepo,
    FakeCommentRepo,
    FakePostToCommentRepo,
)


class TestDatabase:
    @pytest.fixture(scope="function", autouse=True)
    async def seed_fake_entities(self):
        await MigrateFakeEntities().setup()
        yield
        await MigrateFakeEntities().teardown()

    async def test_insert_and_fetch(self):
        author = await FakeAuthorRepo.create_return({"name": "fake name"})
        assert author.name == "fake name jr."

    async def test_update_and_fetch(self):
        author = await FakeAuthorRepo.create_return({"name": "fake name"})

        await FakeAuthorRepo.update_by_id(author.id, {"name": "another fake name"})

        author = await FakeAuthorRepo.find_by_id(author.id)

        assert author.name == "another fake name jr."

    async def test_delete_and_count(self):
        await FakeAuthorRepo.create_return({"name": "fake name"})

        assert len(await FakeAuthorRepo.all()) == 1

        (await postgres.acquire()).allow_wildcard_queries()
        await FakeAuthorRepo.execute(FakeAuthorRepo.delete_query())
        (await postgres.acquire()).deny_wildcard_queries()

        assert len(await FakeAuthorRepo.all()) == 0

    async def test_has_many_relation(self):
        author = await FakeAuthorRepo.create_return({"name": "fake name"})

        await FakePostRepo.create_many(
            [
                {"author_id": author.id, "title": "First Post"},
                {"author_id": author.id, "title": "Second Post"},
                {"author_id": author.id, "title": "Third Post"},
            ]
        )

        await FakeAuthorRepo.apply_relation(author, "posts")
        assert len(author.posts) == 3

    async def test_nested_relation(self):
        author = await FakeAuthorRepo.create_return({"name": "fake name"})

        post = await FakePostRepo.create_return(
            {"author_id": author.id, "title": "First Post"}
        )

        tags = await FakeTagRepo.create_return_many(
            [
                {"title": "First Tag"},
                {"title": "Second Tag"},
            ]
        )

        await FakePostToTagRepo.create_many(
            [
                {"tag_id": tags[0].id, "post_id": post.id},
                {"tag_id": tags[1].id, "post_id": post.id},
            ]
        )

        await FakeAuthorRepo.apply_relation(author, "posts.tags")

        assert len(author.posts[0].tags) == 2

    async def test_belongs_to_relation(self):
        post = await FakePostRepo.create_return({"author_id": 1, "title": "First Post"})

        tags = await FakeTagRepo.create_return_many(
            [
                {"title": "First Tag"},
                {"title": "Second Tag"},
            ]
        )

        await FakePostToTagRepo.create_return_many(
            [
                {"tag_id": tags[0].id, "post_id": post.id},
                {"tag_id": tags[1].id, "post_id": post.id},
            ]
        )

        await FakePostRepo.apply_relation(post, "tags")

        assert len(post.tags) == 2

    async def test_json_belongs_to_relation(self):
        post = await FakePostRepo.create_return({"author_id": 1, "title": "First Post"})

        metadata = {"nested": {"pinned_post_id": post.id}}
        author = await FakeAuthorRepo.create_return(
            {"name": "John", "metadata": metadata}
        )
        await FakeAuthorRepo.apply_relation(author, "pinned_post")
        assert author.pinned_post.id == post.id

        metadata = {"nested": {}}
        author = await FakeAuthorRepo.create_return(
            {"name": "Jane", "metadata": metadata}
        )
        await FakeAuthorRepo.apply_relation(author, "pinned_post")
        assert author.pinned_post is None

    async def test_relations_are_cached(self):
        post = await FakePostRepo.create_return({"author_id": 1, "title": "First Post"})

        tags = await FakeTagRepo.create_return_many(
            [
                {"title": "First Tag"},
                {"title": "Second Tag"},
            ]
        )

        await FakePostToTagRepo.create_many(
            [
                {"tag_id": tags[0].id, "post_id": post.id},
                {"tag_id": tags[1].id, "post_id": post.id},
            ]
        )

        await FakePostRepo.apply_relation(post, "tags")

        post.tags[0].title = "First Tag Edited"

        await FakePostRepo.apply_relation(post, "tags")

        assert post.tags[0].title == "First Tag Edited"

    async def test_missing_relation(self):
        post = await FakePostRepo.create_return({"author_id": 1, "title": "First Post"})

        await FakePostRepo.apply_relation(post, "author")

        assert post.author is None

    async def test_relation_callback(self):
        author = await FakeAuthorRepo.create_return({"name": "Fake Name"})

        await FakePostRepo.create_many(
            [
                {"author_id": author.id, "title": "First Post", "is_active": 1},
                {"author_id": author.id, "title": "First Post", "is_active": 0},
            ]
        )

        await FakeAuthorRepo.apply_relations(author, ["posts", "active_posts"])

        assert len(author.posts) == 2
        assert len(author.active_posts) == 1

    async def test_relations_do_not_reset_models(self):
        author = await FakeAuthorRepo.create_return({"name": "Fake Name"})
        post = await FakePostRepo.create_return(
            {"author_id": author.id, "title": "First Post", "is_active": 1}
        )
        tags = await FakeTagRepo.create_return_many(
            [{"title": "First Tag"}, {"title": "Second Tag"}]
        )
        await FakePostToTagRepo.create_many(
            [
                {"tag_id": tags[0].id, "post_id": post.id},
                {"tag_id": tags[1].id, "post_id": post.id},
            ]
        )
        comments = await FakeCommentRepo.create_return_many(
            [{"comment": "First Comment"}, {"comment": "Second Comment"}]
        )
        await FakePostToCommentRepo.create_many(
            [
                {"comment_id": comments[0].id, "post_id": post.id},
                {"comment_id": comments[1].id, "post_id": post.id},
            ]
        )
        await FakeAuthorRepo.apply_relations(author, ["posts.tags", "posts.comments"])
        # print(author)
        assert author.posts[0].tags[0].title == "First Tag"
        assert author.posts[0].comments[0].comment == "First Comment"

    async def test_update_model(self):
        author = await FakeAuthorRepo.create_return({"name": "Fake Name"})
        author.name = "Andy"
        await FakeAuthorRepo.update_model(author, ["name"])

        assert author.name == "Andy"
        assert author.x_original["name"] == "Andy"

    async def test_delete_by_id(self):
        authors = await FakeAuthorRepo.create_return_many(
            [
                {"name": "A"},
                {"name": "B"},
                {"name": "C"},
            ]
        )
        ids = [author.id for author in authors]
        await FakeAuthorRepo.delete_by_id(ids)
        count = await FakeAuthorRepo.count(
            FakeAuthorRepo.select_where(FakeAuthorRepo.field("id").isin(ids))
        )

        assert count == 0

    async def test_delete_models(self):
        authors = await FakeAuthorRepo.create_return_many(
            [
                {"name": "A"},
                {"name": "B"},
                {"name": "C"},
            ]
        )
        ids = [author.id for author in authors]
        await FakeAuthorRepo.delete_models(authors)
        count = await FakeAuthorRepo.count(
            FakeAuthorRepo.select_where(FakeAuthorRepo.field("id").isin(ids))
        )

        assert count == 0

    async def test_transaction_rollback(self):
        prev_total = await FakeAuthorRepo.count(FakeAuthorRepo.select_query())
        await (await postgres.acquire()).begin_transaction()
        await FakeAuthorRepo.create(attributes=dict(name='Akber'))
        await FakeAuthorRepo.create(attributes=dict(name='Asgher'))
        await (await postgres.acquire()).rollback_transaction()
        new_total = await FakeAuthorRepo.count(FakeAuthorRepo.select_query())
        assert prev_total == new_total

    async def test_transaction_commit(self):
        prev_total = await FakeAuthorRepo.count(FakeAuthorRepo.select_query())
        await (await postgres.acquire()).begin_transaction()
        await FakeAuthorRepo.create(attributes=dict(name='Akber'))
        await FakeAuthorRepo.create(attributes=dict(name='Asgher'))
        await (await postgres.acquire()).commit_transaction()
        new_total = await FakeAuthorRepo.count(FakeAuthorRepo.select_query())
        assert prev_total + 2 == new_total

    async def test_nested_transaction(self):
        await (await postgres.acquire()).begin_transaction()
        await (await postgres.acquire()).begin_transaction()

        await FakeAuthorRepo.create(attributes=dict(name='Akber'))

        await (await postgres.acquire()).commit_transaction()
        assert (await postgres.acquire()).is_in_transaction is True

        await (await postgres.acquire()).commit_transaction()
        assert (await postgres.acquire()).is_in_transaction is False
