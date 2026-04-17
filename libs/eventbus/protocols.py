import asyncio
from typing import Protocol, TypeVar

E = TypeVar("E")


class Actor(Protocol[E]):
    @property
    def inbox(self) -> asyncio.Queue[E]: ...

    async def run(self) -> None:
        """Long-running task. Drains inbox and processes batches."""
