import json
import os
from dataclasses import dataclass, asdict, field
from typing import List, Optional


@dataclass
class Progress:
    phase: str = "init"
    phase1_start_ts: int = 0
    last_entity_id: Optional[str] = None
    last_entity_ts: int = 0
    watermark_ts: int = 0
    migrated_rows: int = 0
    skipped_rows: int = 0
    started_at: str = ""
    partitioning: str = "MONTHS"
    cast_strings: bool = False
    completed_entities: List[str] = field(default_factory=list)


class ProgressTracker:
    def __init__(self, checkpoint_file: str):
        self.checkpoint_file = checkpoint_file
        self.progress = Progress()

    def load(self) -> bool:
        if not os.path.exists(self.checkpoint_file):
            return False
        with open(self.checkpoint_file) as f:
            data = json.load(f)
        self.progress = Progress(**{k: v for k, v in data.items()
                                    if k in Progress.__dataclass_fields__})
        return True

    def save(self):
        with open(self.checkpoint_file, "w") as f:
            json.dump(asdict(self.progress), f, indent=2)

    def update(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self.progress, k, v)
        self.save()
