from __future__ import annotations

from abc import ABC, abstractmethod


class Notifier(ABC):
    @abstractmethod
    async def send_text(self, text: str) -> None:
        raise NotImplementedError


class NullNotifier(Notifier):
    async def send_text(self, text: str) -> None:
        return None
