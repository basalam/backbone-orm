from enum import Enum


class CustomOrder(Enum):
    desc_null_first = "DESC NULLS FIRST"
    desc_null_last = "DESC NULLS LAST"
    asc_null_first = "ASC NULLS FIRST"
    asc_null_last = "ASC NULLS LAST"
