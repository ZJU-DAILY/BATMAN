from __future__ import annotations

import json
import shutil
from pathlib import Path

from app.config import settings
from app.engine.utils import now_utc, target_mode_from_samples
from app.models import Session


class SessionStorage:
    def __init__(self, root: Path):
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def cleanup_expired(self) -> None:
        for session_dir in self.root.iterdir():
            if not session_dir.is_dir():
                continue
            session_file = session_dir / "session.json"
            if not session_file.exists():
                shutil.rmtree(session_dir, ignore_errors=True)
                continue
            try:
                payload = json.loads(session_file.read_text(encoding="utf-8"))
                session = Session.model_validate(payload)
            except Exception:
                shutil.rmtree(session_dir, ignore_errors=True)
                continue
            if session.expires_at < now_utc():
                shutil.rmtree(session_dir, ignore_errors=True)

    def session_dir(self, session_id: str) -> Path:
        return self.root / session_id

    def get_session_file(self, session_id: str) -> Path:
        return self.session_dir(session_id) / "session.json"

    def save(self, session: Session) -> Session:
        session.mode = target_mode_from_samples(session.target_samples)
        session_dir = self.session_dir(session.id)
        session_dir.mkdir(parents=True, exist_ok=True)
        (session_dir / "sources").mkdir(parents=True, exist_ok=True)
        (session_dir / "exports").mkdir(parents=True, exist_ok=True)
        self.get_session_file(session.id).write_text(
            session.model_dump_json(indent=2),
            encoding="utf-8",
        )
        return session

    def load(self, session_id: str) -> Session:
        session_file = self.get_session_file(session_id)
        if not session_file.exists():
            raise FileNotFoundError(session_id)
        payload = json.loads(session_file.read_text(encoding="utf-8"))
        return Session.model_validate(payload)

    def delete(self, session_id: str) -> None:
        shutil.rmtree(self.session_dir(session_id), ignore_errors=True)


storage = SessionStorage(settings.session_root)
