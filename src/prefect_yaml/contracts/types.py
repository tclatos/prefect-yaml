"""Type definitions and contracts for workflow inputs."""

from __future__ import annotations

from enum import Enum


class ContractType(str, Enum):
    """Supported data types for workflow input contracts."""

    STRING = "string"
    STR = "str"
    INTEGER = "integer"
    INT = "int"
    FLOAT = "float"
    NUMBER = "number"
    BOOLEAN = "boolean"
    BOOL = "bool"
    PATH = "path"
    ENUM = "enum"
    LIST = "list"
    DICT = "dict"
    ANY = "any"
