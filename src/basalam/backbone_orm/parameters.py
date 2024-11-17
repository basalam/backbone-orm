from typing import List, Any, Iterable

from pypika import Parameter


class Parameters:
    def __init__(self, *args) -> None:
        self.__params: List = list()
        for arg in args:
            self.make(arg)

    def make(self, value: Any) -> Parameter:
        self.__params.append(value)
        return Parameter(f"${len(self.__params)}")

    def make_many(self, values: Iterable) -> List[Parameter]:
        return [self.make(value) for value in values]

    def values(self):
        return self.__params

    def bindings(self):
        return [Parameter(f"${index + 1}") for index, value in enumerate(self.__params)]
