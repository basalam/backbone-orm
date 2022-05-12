from abc import ABC, abstractmethod
from typing import List, Dict, Optional, Any


class ApiResourceAbstract(ABC):
    @classmethod
    @abstractmethod
    async def make(cls, model: Any) -> Dict:
        pass

    @classmethod
    async def collection(cls, models: List[Any]) -> List[Dict]:
        return [await cls.make(model) for model in models]

    @classmethod
    async def optional(cls, model: Optional[Any]) -> Optional[Dict]:
        if model is None:
            return None
        else:
            return await cls.make(model)

    @classmethod
    async def first_or_none(cls, models: List[Any]) -> Optional[Dict]:
        if len(models) > 0:
            return await cls.make(models[0])
        else:
            return None
