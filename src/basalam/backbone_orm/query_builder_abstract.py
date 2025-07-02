from abc import ABC
from datetime import datetime, date, time
from decimal import Decimal
from functools import reduce
from typing import Type, TYPE_CHECKING, Union, List, TypeVar, Tuple

from pypika.dialects import PostgreSQLQueryBuilder

from .model_abstract import T
from .model_schema_abstract import ModelSchemaAbstract

if TYPE_CHECKING:
    from . import RepositoryAbstract


class DataTypes:
    NUMERIC = [int, float, Decimal]
    CHAR = [str, bytes]
    DATETIME = [datetime, date, time]


class Filters:
    EQUALITY = 'eq'
    NOT_EQUALITY = 'ne'
    GREATER_THAN = 'gt'
    GREATER_THAN_OR_EQUAL = 'gte'
    LESS_THAN = 'lt'
    LESS_THAN_OR_EQUAL = 'le'
    CONTAINS = 'contains'
    ICONTAINS = 'icontains'
    IN = 'in'
    NOT_IN = 'nin'

    @classmethod
    def iterables(cls) -> Tuple:
        return cls.IN, cls.NOT_IN

    @classmethod
    def numeric(cls) -> Tuple:
        return (cls.EQUALITY, cls.NOT_EQUALITY, cls.GREATER_THAN, cls.GREATER_THAN_OR_EQUAL, cls.LESS_THAN,
                cls.LESS_THAN_OR_EQUAL, cls.IN, cls.NOT_IN)

    @classmethod
    def character(cls) -> Tuple:
        return cls.IN, cls.CONTAINS, cls.ICONTAINS, cls.NOT_IN


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
        if id is None:
            return self
        return self.where(self.repo.field(self.repo.identifier()).isin(self.ensure_list(id)))

    def filter_min_created_at(self, timestamp: int):
        if timestamp is None:
            return self
        return self.where(
            self.repo.field(self.repo.created_at_field()).gte(
                datetime.fromtimestamp(timestamp))
        )

    def filter_max_created_at(self, timestamp: int):
        if timestamp is None:
            return self
        return self.where(
            self.repo.field(self.repo.created_at_field()).lte(
                datetime.fromtimestamp(timestamp))
        )

    def select_star(self):
        return self.select(self.repo.table().star)

    def model(self) -> Type[T]:
        # Delegate to the repository's model method.
        return self.repo.model()

    def schema(self) -> Type[ModelSchemaAbstract]:
        # Delegate to the repository's schema method.
        return self.repo.schema()

    def validate_fields(self, filters: dict):
        """
        Validate that all keys in the filters dictionary correspond to fields in the Pydantic model.

        The key can optionally include a lookup suffix (e.g., 'name__icontains'). This method
        extracts the field name (the part before the first '__') and checks if it is defined in the model.

        Raises:
            ValueError: If a key (or its corresponding field) is not found in the model.
        """
        model_cls = self.model()
        allowed_fields = set(model_cls.model_fields.keys())

        for key in filters:
            # Extract the actual field name before any lookup suffix
            field_name = key.split('__', 1)[0]
            if field_name not in allowed_fields:
                raise ValueError(
                    f"Field '{field_name}' is not valid for model '{model_cls.__name__}'. "
                    f"Allowed fields: {allowed_fields}"
                )

    def validate_value(self, filters: dict):
        """
        Validate that each filter key is a valid field in the model and that the lookup operator
        and value type are appropriate for that field's data type.

        Rules enforced in this example:
          - Numeric fields (int, float, Decimal):
              • Allowed operators: those in Filters.numeric()
              • For __in lookup: the value must be a list/tuple of numeric values.
              • Otherwise: the value must be numeric.
          - Character fields (str, bytes):
              • Allowed operators: __contains, __icontains, __in (aside from equality operators).
              • For __in lookup: the value must be a list/tuple of strings (or bytes).
              • Otherwise: the value must be a str or bytes.
          - Datetime fields:
              • For __in lookup: the value must be a list/tuple of datetime/date/time.
              • Otherwise: the value must be a datetime, date, or time.

        Raises:
            ValueError: If a field is invalid or if an operator/value combination does not match
                        the expected data type.
        """
        model_cls = self.model()
        allowed_fields = model_cls.model_fields

        for key, value in filters.items():
            # Split the key: the part before the first '__' is the field name;
            # if no lookup is provided, default to equality.
            parts = key.split('__', 1)
            if len(parts) == 1:
                field_name = parts[0]
                op = Filters.EQUALITY
            else:
                field_name, op = parts

            field_info = allowed_fields[field_name]
            # Use the outer_type_ which reflects the declared type (including things like Optional)
            field_type = field_info.annotation

            # Determine a basic "category" for the field type
            if any(isinstance(field_type, type) and issubclass(field_type, num_type) for num_type in DataTypes.NUMERIC):
                category = 'numeric'
            elif any(
                    isinstance(field_type, type) and issubclass(field_type, char_type) for char_type in DataTypes.CHAR):
                category = 'character'
            elif any(isinstance(field_type, type) and issubclass(field_type, dt_type) for dt_type in
                     DataTypes.DATETIME):
                category = 'datetime'
            else:
                category = 'other'
            # --- Operator validation based on category ---
            # Equality operators are allowed for any type.
            if op not in (Filters.EQUALITY, Filters.NOT_EQUALITY):
                if category == 'numeric':
                    if op not in Filters.numeric():
                        raise ValueError(
                            f"Operator '{op}' is not allowed for numeric field '{field_name}'."
                        )
                elif category == 'character':
                    if op not in Filters.character():
                        raise ValueError(
                            f"Operator '{op}' is not allowed for character field '{field_name}'."
                        )
                elif category == 'datetime':
                    # For this example, we disallow __contains lookups on datetime fields.
                    if op in (Filters.CONTAINS, Filters.ICONTAINS):
                        raise ValueError(
                            f"Operator '{op}' is not allowed for datetime field '{field_name}'."
                        )
                # For other types, you may wish to implement additional rules.

            # --- Value type validation ---
            if op in Filters.iterables():
                if not isinstance(value, (list, tuple)):
                    raise ValueError(
                        f"Operator '{op}' requires a list or tuple as value for field '{field_name}'."
                    )
                # Validate each element in the list/tuple.
                if category == 'numeric':
                    for item in value:
                        if not isinstance(item, tuple(DataTypes.NUMERIC)):
                            raise ValueError(
                                f"Each item for field '{field_name}' must be numeric when using '{op}'."
                            )
                elif category == 'character':
                    for item in value:
                        if not isinstance(item, (str, bytes)):
                            raise ValueError(
                                f"Each item for field '{field_name}' must be a string or bytes when using '{op}'."
                            )
                elif category == 'datetime':
                    for item in value:
                        if not isinstance(item, (datetime, date, time)):
                            raise ValueError(
                                f"Each item for field '{field_name}' must be a datetime, date, or time when using '{op}'."
                            )
            else:
                # For non __in operators, check that the value itself is of the expected type.
                if category == 'numeric':
                    if not isinstance(value, tuple(DataTypes.NUMERIC)):
                        raise ValueError(
                            f"Value for field '{field_name}' must be numeric for operator '{op}'."
                        )
                elif category == 'character':
                    if not isinstance(value, (str, bytes)):
                        raise ValueError(
                            f"Value for field '{field_name}' must be a string or bytes for operator '{op}'."
                        )
                elif category == 'datetime':
                    if not isinstance(value, (datetime, date, time)):
                        raise ValueError(
                            f"Value for field '{field_name}' must be a datetime, date, or time for operator '{op}'."
                        )
        # All validations passed.

    def clean_filters(self, filters: dict) -> dict:
        cleaned_filters = {}
        for field in self.model().model_fields.keys():
            cleaned_filters[field] = []
        for key, value in filters.items():
            parts = key.split('__', 1)
            if len(parts) == 1:
                field_name = self.get_schema_field_from_filter(parts[0])
                operator = Filters.EQUALITY
                cleaned_filters[field_name].append(
                    {
                        'operator': operator,
                        'value': value,
                    }
                )
            else:
                field_name, operator = parts
                cleaned_filters[field_name].append(
                    {
                        'operator': operator,
                        'value': value,
                    }
                )
        for field in list(cleaned_filters.keys()):
            if not cleaned_filters[field]:
                cleaned_filters.pop(field)
        return cleaned_filters

    def get_schema_field_from_filter(self, filter_field: str) -> str:
        """
        Given a filter field name (e.g. "created_at"), return the corresponding schema attribute.
        For example, if filter_field is "created_at" and schema_cls has an attribute
        CREATED_AT = "created_at", this function returns schema_cls.CREATED_AT.

        Args:
            filter_field (str): The filter field in lowercase.

        Returns:
            str: The value of the schema attribute (e.g. "created_at").

        Raises:
            ValueError: If no matching field is found in the schema.
        """
        # Normalize the filter field to lowercase.
        filter_field = filter_field.lower()
        # Iterate over the attributes of the schema class.
        for attr in dir(self.schema()):
            # We assume schema fields are defined as uppercase class attributes.
            if attr.isupper():
                value = getattr(self.schema(), attr)
                # Check if the schema attribute's value matches the filter field.
                if isinstance(value, str) and value.lower() == filter_field:
                    # Return the schema field (i.e. C2CSchema.CREATED_AT for "created_at").
                    return value

        raise ValueError(
            f"Field '{filter_field}' not found in schema '{self.schema().__name__}'.")

    def filter_query(self, **kwargs):
        self.validate_fields(kwargs)
        self.validate_value(kwargs)
        cleaned_filters = self.clean_filters(kwargs)
        criterions = []
        for field, filter_set in cleaned_filters.items():
            for filter_ in filter_set:
                operator = filter_['operator']
                value = filter_['value']
                match operator:
                    case Filters.EQUALITY:
                        criterions.append(self.repo.field(field).eq(value))
                    case Filters.NOT_EQUALITY:
                        criterions.append(self.repo.field(field).ne(value))
                    case Filters.IN:
                        criterions.append(self.repo.field(field).isin(value))
                    case Filters.NOT_IN:
                        criterions.append(self.repo.field(field).notin(value))
                    case Filters.CONTAINS:
                        criterions.append(
                            self.repo.field(field).contains(value))
                    case Filters.ICONTAINS:
                        criterions.append(
                            self.repo.field(field).contains(value))
                    case Filters.GREATER_THAN:
                        criterions.append(self.repo.field(field).gt(value))
                    case Filters.GREATER_THAN_OR_EQUAL:
                        criterions.append(self.repo.field(field).gte(value))
                    case Filters.LESS_THAN:
                        criterions.append(self.repo.field(field).lt(value))
                    case Filters.LESS_THAN_OR_EQUAL:
                        criterions.append(self.repo.field(field).lte(value))
        query = self.where(
            reduce(lambda a, b: a & b, criterions)
        )
        return query


V = TypeVar("V", bound=QueryBuilderAbstract)
