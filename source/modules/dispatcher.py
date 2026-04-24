import logging
from typing import Dict, Any, List

from source.core.config import Config
from source.modules.database_actions import DatabaseActions

logger = logging.getLogger(__name__)

class ActionDispatcher:
    def __init__(self, config: Config, db_actions: DatabaseActions):
        self.db = db_actions

    def execute_actions(self, actions: List[Dict[str, Any]], telegram_id: int) -> List[Dict[str, Any]]:
        results = []
        for action in actions:
            action_type = action.get("type")
            params = action.get("params", {})
            method = getattr(self.db, action_type, None)
            if method is None:
                logger.error(f"Неизвестное действие: {action_type}")
            result = method(telegram_id, params)
            results.append({"action": action_type, "result": result})
        return results