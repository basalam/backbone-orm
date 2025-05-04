from __future__ import annotations

import abc
from typing import Optional, Dict, Sequence, Tuple, Any, TYPE_CHECKING, Set, List, TypeVar

from pydantic import ConfigDict, BaseModel

if TYPE_CHECKING:
    pass


class RelationNotAppliedException(Exception):
    pass


class ModelAbstract(BaseModel, abc.ABC):
    x_ref: Optional[int] = None
    x_applied_relations: Set[str] = set()
    x_original: Dict = {}
    x_relations: Dict = {}
    model_config = ConfigDict(validate_assignment=True)

    def dict(self, **kwargs):
        hidden_fields = set(
            attribute_name
            for attribute_name in self.model_fields.keys()
            if attribute_name[0:2] == "x_"
        )
        kwargs.setdefault("exclude", hidden_fields)
        return super().model_dump(**kwargs)

    def __repr_args__(self) -> Sequence[Tuple[Optional[str], Any]]:
        return [
            (key, self.__filter_fn(value))
            for key, value in super().__repr_args__()
            if key[0:2] != "x_"
        ]

    def __str__(self):
        return type(self).__name__ + "\n  " + (self.__repr_str__("\n  "))

    def __filter_fn(self, value):
        if isinstance(value, str) and len(value) > 50:
            return value[:50] + "..."

        if (
            isinstance(value, list)
            and len(value) > 0
            and isinstance(value[0], ModelAbstract)
        ):
            return [self.__format_relation(relation) for relation in value]

        if isinstance(value, BaseModel):
            return self.__format_relation(value)

        return value

    def __format_relation(self, value):
        try:
            return type(value).__name__ + "#" + str(value.__getattribute__("id"))
        except AttributeError:
            return type(value).__name__

    def original(self):
        return self.x_original

    def set_relation(self, name, value):
        self.x_relations[name] = value
        self.x_applied_relations.add(name)

    def forget_relation(self, name):
        if name in self.x_relations.keys():
            del self.x_relations[name]

        if name in self.x_applied_relations:
            self.x_applied_relations.remove(name)

    def relation(self, name: str):
        if name not in self.x_applied_relations:
            raise RelationNotAppliedException(f"Relation {name} not applied")
        return self.x_relations.get(name)

    def has_relation(self, name: str):
        return name in self.x_relations.keys()

    def has_relations(self, names: List[str]):
        return all(name in self.x_relations.keys() for name in names)


T = TypeVar("T", bound=ModelAbstract)
