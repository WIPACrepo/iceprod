from dataclasses import dataclass
from ..grid import BaseGrid, GridTask


@dataclass(kw_only=True, slots=True)
class TestTask(GridTask):
    dataset_id: str | None = None
    task_id: str | None = None
    instance_id: str | None = None


class Grid(BaseGrid):
    pass
