"""Forward-time splits with transitive event/market/wallet isolation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable

from .schemas import EvaluationRow, unique_evaluation_rows


@dataclass(frozen=True, slots=True)
class DatasetSplit:
    train: tuple[EvaluationRow, ...]
    validation: tuple[EvaluationRow, ...]
    test: tuple[EvaluationRow, ...]
    dropped_cross_boundary: tuple[EvaluationRow, ...]


class _UnionFind:
    def __init__(self, size: int):
        self.parent = list(range(size))

    def find(self, item: int) -> int:
        while self.parent[item] != item:
            self.parent[item] = self.parent[self.parent[item]]
            item = self.parent[item]
        return item

    def union(self, left: int, right: int) -> None:
        left_root, right_root = self.find(left), self.find(right)
        if left_root != right_root:
            self.parent[right_root] = left_root


def _components(rows: list[EvaluationRow]) -> list[list[EvaluationRow]]:
    union = _UnionFind(len(rows))
    owners: dict[tuple[str, str], int] = {}
    for index, row in enumerate(rows):
        keys = [
            ("event", row.event_cluster_uid),
            ("market", row.market_uid),
            *(("actor", actor) for actor in row.actor_uids),
        ]
        for key in keys:
            previous = owners.setdefault(key, index)
            union.union(index, previous)
    grouped: dict[int, list[EvaluationRow]] = {}
    for index, row in enumerate(rows):
        grouped.setdefault(union.find(index), []).append(row)
    return list(grouped.values())


def forward_disjoint_split(
    rows: Iterable[EvaluationRow],
    *,
    train_end: datetime,
    validation_end: datetime,
) -> DatasetSplit:
    if train_end.tzinfo is None or validation_end.tzinfo is None:
        raise ValueError("split boundaries must be timezone-aware")
    if validation_end <= train_end:
        raise ValueError("validation_end must be after train_end")
    partitions: dict[str, list[EvaluationRow]] = {
        "train": [], "validation": [], "test": [], "dropped": []
    }
    for component in _components(list(unique_evaluation_rows(rows))):
        earliest = min(row.event_time for row in component)
        latest = max(row.event_time for row in component)
        if latest < train_end:
            destination = "train"
        elif earliest >= train_end and latest < validation_end:
            destination = "validation"
        elif earliest >= validation_end:
            destination = "test"
        else:
            destination = "dropped"
        partitions[destination].extend(component)
    result = DatasetSplit(
        train=tuple(sorted(partitions["train"], key=lambda row: row.event_time)),
        validation=tuple(sorted(partitions["validation"], key=lambda row: row.event_time)),
        test=tuple(sorted(partitions["test"], key=lambda row: row.event_time)),
        dropped_cross_boundary=tuple(sorted(partitions["dropped"], key=lambda row: row.event_time)),
    )
    assert_no_split_leakage(result)
    return result


def assert_no_split_leakage(split: DatasetSplit) -> None:
    partitions = {"train": split.train, "validation": split.validation, "test": split.test}
    names = list(partitions)
    for left_index, left_name in enumerate(names):
        left = partitions[left_name]
        for right_name in names[left_index + 1:]:
            right = partitions[right_name]
            for attribute in ("market_uid", "event_cluster_uid"):
                overlap = {getattr(row, attribute) for row in left} & {
                    getattr(row, attribute) for row in right
                }
                if overlap:
                    raise AssertionError(f"{attribute} leakage between {left_name} and {right_name}: {overlap}")
            left_actors = {actor for row in left for actor in row.actor_uids}
            right_actors = {actor for row in right for actor in row.actor_uids}
            if left_actors & right_actors:
                raise AssertionError(f"actor leakage between {left_name} and {right_name}")
    if split.train and split.validation and max(row.event_time for row in split.train) >= min(
        row.event_time for row in split.validation
    ):
        raise AssertionError("train is not strictly earlier than validation")
    if split.validation and split.test and max(row.event_time for row in split.validation) >= min(
        row.event_time for row in split.test
    ):
        raise AssertionError("validation is not strictly earlier than test")
    if split.train and split.test and max(row.event_time for row in split.train) >= min(
        row.event_time for row in split.test
    ):
        raise AssertionError("train is not strictly earlier than test")
