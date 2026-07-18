"""Append-only case/adjudication/window registry with explicit supersession."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Iterable, TypeVar

from pydantic import BaseModel

from .schemas import Adjudication, Case, LabelTarget, LabelWindow

Record = Case | Adjudication | LabelWindow
T = TypeVar("T", Case, Adjudication, LabelWindow)


class LabelRegistry:
    def __init__(self, path: str | Path):
        self.path = Path(path)

    def _rows(self) -> list[dict]:
        if not self.path.exists():
            return []
        return [json.loads(line) for line in self.path.read_text(encoding="utf-8").splitlines() if line.strip()]

    def _uids(self) -> set[str]:
        result: set[str] = set()
        for row in self._rows():
            payload = row["payload"]
            for name in ("case_uid", "adjudication_uid", "window_uid"):
                if name in payload:
                    result.add(payload[name])
        return result

    def append(self, record: Record) -> None:
        uid_name = {
            Case: "case_uid",
            Adjudication: "adjudication_uid",
            LabelWindow: "window_uid",
        }[type(record)]
        uid = getattr(record, uid_name)
        rows = self._rows()
        if uid in self._uids():
            raise ValueError(f"registry UID already exists: {uid}")
        if isinstance(record, Adjudication):
            cases = {row["payload"].get("case_uid") for row in rows if row["kind"] == "case"}
            if record.case_uid not in cases:
                raise ValueError(f"unknown case_uid: {record.case_uid}")
            adjudication_rows = [row for row in rows if row["kind"] == "adjudication"]
            superseded_uids = {
                row["payload"].get("supersedes_uid")
                for row in adjudication_rows if row["payload"].get("supersedes_uid")
            }
            current_same_target = [
                row for row in adjudication_rows
                if row["payload"].get("case_uid") == record.case_uid
                and row["payload"].get("target") == record.target.value
                and row["payload"].get("adjudication_uid") not in superseded_uids
            ]
            if current_same_target and record.supersedes_uid is None:
                raise ValueError("a new case/target adjudication must supersede the current one")
            if record.supersedes_uid:
                previous_rows = [
                    row for row in rows
                    if row["kind"] == "adjudication"
                    and row["payload"].get("adjudication_uid") == record.supersedes_uid
                ]
                if not previous_rows:
                    raise ValueError("supersedes_uid does not reference an adjudication")
                previous = Adjudication.model_validate(previous_rows[0]["payload"], strict=False)
                if previous.case_uid != record.case_uid or previous.target != record.target:
                    raise ValueError("supersession must retain case and A/B/C target")
                already_superseded = record.supersedes_uid in superseded_uids
                if already_superseded:
                    raise ValueError("an adjudication may have only one direct successor")
        if isinstance(record, LabelWindow):
            cases = {row["payload"].get("case_uid") for row in rows if row["kind"] == "case"}
            if record.case_uid not in cases:
                raise ValueError(f"unknown case_uid: {record.case_uid}")
        envelope = {
            "kind": "case" if isinstance(record, Case) else "adjudication" if isinstance(record, Adjudication) else "window",
            "payload": record.model_dump(mode="json"),
        }
        data = json.dumps(envelope, sort_keys=True, separators=(",", ":")) + "\n"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(self.path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o644)
        try:
            os.write(descriptor, data.encode("utf-8"))
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def cases(self) -> list[Case]:
        return [Case.model_validate(row["payload"], strict=False) for row in self._rows() if row["kind"] == "case"]

    def windows(self) -> list[LabelWindow]:
        return [LabelWindow.model_validate(row["payload"], strict=False) for row in self._rows() if row["kind"] == "window"]

    def adjudications(self, *, current_only: bool = False) -> list[Adjudication]:
        values = [
            Adjudication.model_validate(row["payload"], strict=False)
            for row in self._rows() if row["kind"] == "adjudication"
        ]
        if not current_only:
            return values
        superseded = {value.supersedes_uid for value in values if value.supersedes_uid}
        return [value for value in values if value.adjudication_uid not in superseded]

    def current_adjudication(self, case_uid: str, target: LabelTarget) -> Adjudication | None:
        matches = [
            value for value in self.adjudications(current_only=True)
            if value.case_uid == case_uid and value.target == target
        ]
        if len(matches) > 1:
            raise ValueError("multiple unsuperseded adjudications for case/target")
        return matches[0] if matches else None
