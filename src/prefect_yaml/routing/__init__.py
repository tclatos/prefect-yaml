"""Routing package providing partition and switch routers."""

from __future__ import annotations

from prefect_yaml.models.compiled import RouterRule, RouterSpec
from prefect_yaml.routing.matcher import expand_brace_pattern, matches_pattern
from prefect_yaml.routing.partitioner import (
    RoutingError,
    execute_partition_router,
    partition_items,
)
from prefect_yaml.routing.switch import evaluate_switch

__all__ = [
    "RouterRule",
    "RouterSpec",
    "RoutingError",
    "evaluate_switch",
    "execute_partition_router",
    "expand_brace_pattern",
    "matches_pattern",
    "partition_items",
]
