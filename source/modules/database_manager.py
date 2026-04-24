import sqlite3
import logging
import os
from typing import Optional, List, Dict, Any
from source.core.config import Config


logger = logging.getLogger(__name__)


class DatabaseManager:
    """
    Универсальный менеджер для работы с SQLite.
    Принимает путь к БД и опциональный путь к SQL-скрипту для инициализации схемы.
    """

    def __init__(self, config: Config):
        """
        :param db_path: путь к файлу базы данных (например, "data/app.db")
        :param init_script_path: путь к .sql файлу с CREATE TABLE и т.п. (если нужно)
        """
        self.db_path = config.get('database.path')
        if self.db_path:
            logger.info(f"DatabaseManager инициализирован, БД: {self.db_path}")
        else:
            logger.error("Не указан database.path в конфиге")

        self.init_script_path = config.get('database.init_script')
        self._init_db()

    def _get_connection(self):
        """Возвращает новое соединение с БД."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _init_db(self):
        """Создаёт таблицы, если их нет, выполняя SQL из init_script_path."""
        if not self.init_script_path:
            return
        if not os.path.exists(self.init_script_path):
            logger.error(f"Скрипт инициализации БД не найден: {self.init_script_path}")
            return

        with open(self.init_script_path, 'r', encoding='utf-8') as f:
            sql_script = f.read()

        with self._get_connection() as conn:
            conn.executescript(sql_script)
            conn.commit()
        logger.info(f"База данных инициализирована скриптом: {self.init_script_path}")

    def execute(self, query: str, params: tuple = (), fetch: bool = False):
        """
        Выполняет SQL-запрос.
        Если fetch=True — возвращает список словарей.
        Если fetch=False — возвращает lastrowid для INSERT, иначе None.
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)

            if fetch:
                rows = cursor.fetchall()
                return [dict(row) for row in rows]

            conn.commit()
            return cursor.lastrowid

    def execute_many(self, query: str, params_list: List[tuple]) -> None:
        """Выполняет массовый запрос (например, INSERT многих строк)."""
        with self._get_connection() as conn:
            conn.executemany(query, params_list)
            conn.commit()

    def get_table_schema(self, table: str) -> List[Dict]:
        """
        Возвращает информацию о колонках таблицы через PRAGMA table_info.
        Каждый элемент: cid, name, type, notnull, dflt_value, pk.
        """
        return self.execute(f"PRAGMA table_info({table})", fetch=True)

    def table_exists(self, table: str) -> bool:
        """Проверяет, существует ли таблица."""
        result = self.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table,), fetch=True
        )
        return len(result) > 0