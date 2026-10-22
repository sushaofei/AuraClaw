"""Small lexical reference memory. This is not the AuraClaw production Memory service."""

from __future__ import annotations

import copy
import json
import math
import re
from collections import Counter
from typing import Any


class LexicalReferenceMemory:
    def __init__(self, *, top_k: int = 5) -> None:
        if top_k < 1:
            raise ValueError("top_k must be positive")
        self._top_k = top_k
        self._sessions: list[dict[str, Any]] = []
        self._terms: list[Counter[str]] = []
        self._document_frequency: Counter[str] = Counter()
        self._initialized = False

    async def ingest(self, sessions: list[dict[str, Any]]) -> None:
        if self._initialized:
            raise RuntimeError("Memory namespace cannot be reused")
        self._initialized = True
        self._sessions = copy.deepcopy(sessions)
        self._terms = [Counter(re.findall(r"\w+", json.dumps(s).lower())) for s in sessions]
        for terms in self._terms:
            self._document_frequency.update(terms.keys())

    async def search(self, query: str) -> str:
        if not self._initialized:
            raise RuntimeError("Memory was not initialized")
        terms = set(re.findall(r"\w+", query.lower()))
        scores = [
            sum(
                (1 + math.log(counts[t]))
                * math.log(1 + len(self._sessions) / self._document_frequency[t])
                for t in terms
                if counts[t]
            )
            for counts in self._terms
        ]
        ranked = sorted(range(len(scores)), key=lambda i: (-scores[i], i))[: self._top_k]
        return json.dumps([self._sessions[i] for i in ranked if scores[i] > 0])

    async def close(self) -> None:
        self._sessions.clear()
        self._terms.clear()
        self._document_frequency.clear()
