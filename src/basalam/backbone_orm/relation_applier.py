from __future__ import annotations

from typing import Union, List, Type, TYPE_CHECKING, Tuple

from .model_abstract import T
from .relation import Relation

if TYPE_CHECKING:
    from basalam.backbone_orm.repository_abstract import RepositoryAbstract


class RelationApplier:
    counter = 0

    def __init__(
            self,
            repository: Type["RepositoryAbstract"],
            models: Union[T, List[T]],
            relation: str,
    ) -> None:
        self.__relation = relation.replace("_relation", "")
        self.__is_list = type(models) is list
        self.__models = models if isinstance(models, list) else [models]
        self.__original_models = self.__models
        self.__repository = repository
        RelationApplier.counter += 1

    @property
    def __split_relation(self) -> Tuple[str, str]:
        (current, _, remaining) = self.__relation.partition(".")
        return current, remaining

    @property
    def __current_relation_name(self):
        return self.__split_relation[0]

    @property
    def __current_relation(self) -> Relation:
        name = self.filter_relation_name(self.__current_relation_method_name)
        return getattr(self.__repository, name)()

    def filter_relation_name(self, name):
        return name

    @property
    def __current_relation_repo(self) -> RepositoryAbstract:
        return self.__current_relation.relation_repo

    @property
    def __current_relation_method_name(self) -> str:
        return self.__current_relation_name + "_relation"

    @property
    def __remaining_relation(self):
        return self.__split_relation[1]

    @property
    def __current_repo_name(self):
        return self.__repository.__name__

    @property
    def __applicable_models(self) -> List[T]:
        try:
            return [
                model
                for model in self.__models
                if self.__current_relation_name not in model.x_applied_relations
            ]
        except AttributeError as e:
            print(self.__models)
            raise e

    async def apply(self):
        applicable_models = self.__applicable_models

        if len(applicable_models) != 0:
            await self.__current_relation.apply_many(
                self.__current_relation_name, applicable_models
            )

        for model in applicable_models:
            model.x_applied_relations.add(self.__current_relation_name)

        if self.__remaining_relation != "":
            await self.__branch()

        return self.__original_models if self.__is_list else self.__original_models[0]

    @property
    def __applicable_relation_models(self) -> List[T]:
        applicable_relation_models = []
        for model in self.__models:
            model_relation = self.__model_relation(model)

            if model_relation is not None and isinstance(model_relation, (list, set)):
                applicable_relation_models.extend(model_relation)
            elif model_relation is not None:
                applicable_relation_models.append(model_relation)

        return applicable_relation_models

    def __model_relation(self, model):
        return getattr(model, self.filter_relation_name(self.__current_relation_name))

    async def __branch(self):
        models = self.__applicable_relation_models
        relation = self.__remaining_relation
        repository = self.__current_relation_repo

        self.__models = models
        self.__relation = relation
        self.__repository = repository

        await self.apply()
