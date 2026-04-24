import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple

from source.core.config import Config
from source.modules.database_manager import DatabaseManager

logger = logging.getLogger(__name__)


class DatabaseActions:
    def __init__(self, config: Config, db_manager: DatabaseManager):
        self.config = config
        self.db = db_manager
        logger.info("DatabaseActions инициализирован")

    def is_user_allowed(self, telegram_id: int) -> bool:
        """Проверяет, есть ли telegram_id в таблице users (разрешён ли пользователь)."""
        rows = self.db.execute(
            "SELECT id FROM users WHERE telegram_id = ?",
            (telegram_id,),
            fetch=True
        )
        return len(rows) > 0

    def _get_user_id(self, telegram_id: int) -> int:
        rows = self.db.execute(
            "SELECT id FROM users WHERE telegram_id = ?",
            (telegram_id,),
            fetch=True
        )
        if rows:
            return rows[0]["id"]

        self.db.execute(
            "INSERT INTO users (telegram_id) VALUES (?)",
            (telegram_id,)
        )
        rows = self.db.execute(
            "SELECT id FROM users WHERE telegram_id = ?",
            (telegram_id,),
            fetch=True
        )
        return rows[0]["id"]

    def _pick_param(self, params: Dict[str, Any], *names, default=None):
        for name in names:
            value = params.get(name)
            if value is not None:
                return value
        return default

    def _normalize_label(self, value: Any) -> str:
        """Нормализует имя в единый стиль: trim + первая буква заглавная."""
        if value is None:
            return value
        text = str(value).strip()
        if not text:
            return text
        return text[0].upper() + text[1:]

    def _parse_datetime(self, value, end_of_day: bool = False) -> int:
        """
        Принимает:
        - UNIX timestamp (int/float/str числа)
        - YYYY-MM-DD
        - YYYY-MM-DD HH:MM:SS
        - today / yesterday / tomorrow
        """
        if value is None:
            return None

        if isinstance(value, (int, float)):
            return int(value)

        s = str(value).strip().lower()
        now = datetime.now()

        if s in ("today", "сегодня"):
            dt = now.replace(
                hour=23 if end_of_day else 0,
                minute=59 if end_of_day else 0,
                second=59 if end_of_day else 0,
                microsecond=999999 if end_of_day else 0
            )
            return int(dt.timestamp())

        if s in ("yesterday", "вчера"):
            base = now - timedelta(days=1)
            dt = base.replace(
                hour=23 if end_of_day else 0,
                minute=59 if end_of_day else 0,
                second=59 if end_of_day else 0,
                microsecond=999999 if end_of_day else 0
            )
            return int(dt.timestamp())

        if s in ("tomorrow", "завтра"):
            base = now + timedelta(days=1)
            dt = base.replace(
                hour=23 if end_of_day else 0,
                minute=59 if end_of_day else 0,
                second=59 if end_of_day else 0,
                microsecond=999999 if end_of_day else 0
            )
            return int(dt.timestamp())

        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(s, fmt)
                if fmt == "%Y-%m-%d":
                    dt = dt.replace(
                        hour=23 if end_of_day else 0,
                        minute=59 if end_of_day else 0,
                        second=59 if end_of_day else 0,
                        microsecond=999999 if end_of_day else 0
                    )
                return int(dt.timestamp())
            except ValueError:
                pass

        try:
            return int(float(s))
        except ValueError:
            raise ValueError(f"Некорректная дата/время: {value}")

    def _normalize_category_ids(self, user_id: int, categories):
        """
        Категории принимаются ТОЛЬКО как id.
        Поддерживает:
        - int
        - str с числом
        - list[int|str|dict]
        """
        if categories is None:
            return []

        if isinstance(categories, (int, str, dict)):
            categories = [categories]

        ids = []
        for item in categories:
            if isinstance(item, dict):
                item = item.get("id")

            try:
                cat_id = int(item)
            except Exception:
                raise ValueError(f"Некорректный id категории: {item}")

            exists = self.db.execute(
                "SELECT 1 FROM categories WHERE id = ? AND user_id = ?",
                (cat_id, user_id),
                fetch=True
            )
            if not exists:
                raise ValueError(f"Категория с id={cat_id} не найдена")

            ids.append(cat_id)

        seen = set()
        unique_ids = []
        for cid in ids:
            if cid not in seen:
                seen.add(cid)
                unique_ids.append(cid)
        return unique_ids

    def _fetch_transaction_snapshot(self, transaction_id: int, user_id: int) -> Optional[Dict[str, Any]]:
        rows = self.db.execute(
            """
            SELECT
                t.id,
                t.name,
                t.description,
                t.amount,
                t.quantity,
                t.datetime,
                GROUP_CONCAT(c.id, ',') as category_ids_raw,
                GROUP_CONCAT(c.name, ', ') as categories_raw
            FROM transactions t
            LEFT JOIN transaction_categories tc ON t.id = tc.transaction_id
            LEFT JOIN categories c ON tc.category_id = c.id
            WHERE t.id = ? AND t.user_id = ?
            GROUP BY t.id
            """,
            (transaction_id, user_id),
            fetch=True
        )
        if not rows:
            return None

        row = rows[0]
        category_ids_raw = row.get("category_ids_raw")
        categories_raw = row.get("categories_raw")

        row["category_ids"] = (
            [int(x) for x in category_ids_raw.split(",") if x.strip()]
            if category_ids_raw else []
        )
        row["categories"] = (
            [x.strip() for x in categories_raw.split(",") if x.strip()]
            if categories_raw else []
        )

        row.pop("category_ids_raw", None)
        row.pop("categories_raw", None)
        return row

    def _build_transaction_filter_sql(
        self,
        user_id: int,
        params: Dict[str, Any],
        alias: str = "t"
    ) -> Tuple[List[str], List[Any]]:
        clauses = [f"{alias}.user_id = ?"]
        args: List[Any] = [user_id]

        categories_list = self._pick_param(params, "categories")
        if categories_list:
            cat_ids = self._normalize_category_ids(user_id, categories_list)
            if not cat_ids:
                clauses.append("1 = 0")
                return clauses, args

            placeholders = ",".join(["?"] * len(cat_ids))
            clauses.append(
                f"""
                EXISTS (
                    SELECT 1
                    FROM transaction_categories tc2
                    WHERE tc2.transaction_id = {alias}.id
                      AND tc2.category_id IN ({placeholders})
                )
                """
            )
            args.extend(cat_ids)

        date_from = self._pick_param(params, "date_from")
        if date_from is not None:
            clauses.append(f"{alias}.datetime >= ?")
            args.append(self._parse_datetime(date_from, end_of_day=False))

        date_to = self._pick_param(params, "date_to")
        if date_to is not None:
            clauses.append(f"{alias}.datetime <= ?")
            args.append(self._parse_datetime(date_to, end_of_day=True))

        amount_from = self._pick_param(params, "amount_from")
        if amount_from is not None:
            clauses.append(f"{alias}.amount >= ?")
            args.append(amount_from)

        amount_to = self._pick_param(params, "amount_to")
        if amount_to is not None:
            clauses.append(f"{alias}.amount <= ?")
            args.append(amount_to)

        name = self._pick_param(params, "name")
        if name:
            clauses.append(f"{alias}.name = ?")
            args.append(self._normalize_label(name))
        else:
            name_contains = self._pick_param(params, "name_contains")
            if name_contains:
                clauses.append(f"{alias}.name LIKE ?")
                args.append(f"%{name_contains}%")

        description_contains = self._pick_param(params, "description_contains")
        if description_contains:
            clauses.append(f"COALESCE({alias}.description, '') LIKE ?")
            args.append(f"%{description_contains}%")

        return clauses, args

    # ========== ТРАНЗАКЦИИ ==========

    def add_transaction(self, telegram_id: int, params: Dict[str, Any]) -> Dict:
        user_id = self._get_user_id(telegram_id)

        amount = self._pick_param(params, "amount")
        name = self._pick_param(params, "name")
        categories = self._pick_param(params, "categories", default=[])
        date_value = self._pick_param(params, "date", "datetime")
        description = self._pick_param(params, "description")
        quantity = self._pick_param(params, "quantity", default=1)

        if amount is None or name is None:
            raise ValueError("Не указаны сумма или название")

        name = self._normalize_label(name)
        if description is not None and isinstance(description, str):
            description = description.strip()

        timestamp = self._parse_datetime(date_value) if date_value is not None else int(datetime.now().timestamp())

        transaction_id = self.db.execute(
            "INSERT INTO transactions (user_id, name, description, amount, quantity, datetime) VALUES (?, ?, ?, ?, ?, ?)",
            (user_id, name, description, amount, quantity, timestamp)
        )

        category_ids = self._normalize_category_ids(user_id, categories)
        for cat_id in category_ids:
            self.db.execute(
                "INSERT INTO transaction_categories (transaction_id, category_id) VALUES (?, ?)",
                (transaction_id, cat_id)
            )

        snapshot = self._fetch_transaction_snapshot(transaction_id, user_id)
        logger.info(f"Добавлена транзакция {transaction_id} для user_id={user_id}")
        return {
            "status": "ok",
            "transaction_id": transaction_id,
            "transaction": snapshot
        }

    def get_transactions(self, telegram_id: int, params: Dict[str, Any]) -> List[Dict]:
        user_id = self._get_user_id(telegram_id)

        query = """
            SELECT
                t.id,
                t.name,
                t.description,
                t.amount,
                t.quantity,
                t.datetime,
                GROUP_CONCAT(c.id, ',') as category_ids_raw,
                GROUP_CONCAT(c.name, ', ') as categories_raw
            FROM transactions t
            LEFT JOIN transaction_categories tc ON t.id = tc.transaction_id
            LEFT JOIN categories c ON tc.category_id = c.id
        """
        clauses, args = self._build_transaction_filter_sql(user_id, params, alias="t")
        query += " WHERE " + " AND ".join(clauses)

        query += " GROUP BY t.id"

        allowed_order_by = {
            "id": "t.id",
            "datetime": "t.datetime",
            "amount": "t.amount",
            "name": "t.name",
            "quantity": "t.quantity",
        }
        order_by_key = str(self._pick_param(params, "order_by", default="datetime")).lower()
        order_by = allowed_order_by.get(order_by_key, "t.datetime")

        order_dir = str(self._pick_param(params, "order_dir", default="DESC")).upper()
        if order_dir not in ("ASC", "DESC"):
            order_dir = "DESC"

        query += f" ORDER BY {order_by} {order_dir}"

        limit = self._pick_param(params, "limit", default=50)
        offset = self._pick_param(params, "offset", default=0)
        limit = min(int(limit), 50)
        offset = int(offset)

        query += " LIMIT ? OFFSET ?"
        args.extend([limit, offset])

        rows = self.db.execute(query, tuple(args), fetch=True)

        for row in rows:
            category_ids_raw = row.get("category_ids_raw")
            categories_raw = row.get("categories_raw")

            row["category_ids"] = (
                [int(x) for x in category_ids_raw.split(",") if x.strip()]
                if category_ids_raw else []
            )
            row["categories"] = (
                [x.strip() for x in categories_raw.split(",") if x.strip()]
                if categories_raw else []
            )

            row.pop("category_ids_raw", None)
            row.pop("categories_raw", None)

        return rows

    def update_transaction(self, telegram_id: int, params: Dict[str, Any]) -> Dict:
        user_id = self._get_user_id(telegram_id)
        transaction_id = self._pick_param(params, "transaction_id", "id")
        if not transaction_id:
            raise ValueError("Не указан transaction_id")

        check = self.db.execute(
            "SELECT id FROM transactions WHERE id = ? AND user_id = ?",
            (transaction_id, user_id),
            fetch=True
        )
        if not check:
            raise ValueError("Транзакция не найдена или принадлежит другому пользователю")

        updates = {}
        if "amount" in params and params["amount"] is not None:
            updates["amount"] = params["amount"]
        if "name" in params and params["name"] is not None:
            updates["name"] = self._normalize_label(params["name"])
        if "date" in params and params["date"] is not None:
            updates["datetime"] = self._parse_datetime(params["date"])
        if "datetime" in params and params["datetime"] is not None:
            updates["datetime"] = self._parse_datetime(params["datetime"])
        if "description" in params and params["description"] is not None:
            updates["description"] = params["description"].strip() if isinstance(params["description"], str) else params["description"]
        if "quantity" in params and params["quantity"] is not None:
            updates["quantity"] = params["quantity"]

        if updates:
            set_clause = ", ".join(f"{k} = ?" for k in updates.keys())
            values = list(updates.values()) + [transaction_id, user_id]
            self.db.execute(
                f"UPDATE transactions SET {set_clause} WHERE id = ? AND user_id = ?",
                tuple(values)
            )

        if "categories" in params:
            cats = params["categories"]
            category_ids = self._normalize_category_ids(user_id, cats)

            self.db.execute(
                "DELETE FROM transaction_categories WHERE transaction_id = ?",
                (transaction_id,)
            )
            for cat_id in category_ids:
                self.db.execute(
                    "INSERT INTO transaction_categories (transaction_id, category_id) VALUES (?, ?)",
                    (transaction_id, cat_id)
                )

        snapshot = self._fetch_transaction_snapshot(transaction_id, user_id)
        logger.info(f"Обновлена транзакция {transaction_id}")
        return {
            "status": "updated",
            "transaction_id": transaction_id,
            "transaction": snapshot
        }

    def delete_transaction(self, telegram_id: int, params: Dict[str, Any]) -> Dict:
        user_id = self._get_user_id(telegram_id)

        transaction_id = self._pick_param(params, "transaction_id", "id")

        if transaction_id is not None:
            rows = self.db.execute(
                """
                SELECT
                    t.id, t.name, t.description, t.amount, t.quantity, t.datetime,
                    GROUP_CONCAT(c.id, ',') as category_ids_raw,
                    GROUP_CONCAT(c.name, ', ') as categories_raw
                FROM transactions t
                LEFT JOIN transaction_categories tc ON t.id = tc.transaction_id
                LEFT JOIN categories c ON tc.category_id = c.id
                WHERE t.id = ? AND t.user_id = ?
                GROUP BY t.id
                """,
                (transaction_id, user_id),
                fetch=True
            )
            if not rows:
                raise ValueError("Транзакция не найдена или принадлежит другому пользователю")

            row = rows[0]
            deleted_snapshot = {
                "id": row["id"],
                "name": row["name"],
                "description": row["description"],
                "amount": row["amount"],
                "quantity": row["quantity"],
                "datetime": row["datetime"],
                "category_ids": [int(x) for x in row["category_ids_raw"].split(",") if x.strip()] if row.get("category_ids_raw") else [],
                "categories": [x.strip() for x in row["categories_raw"].split(",") if x.strip()] if row.get("categories_raw") else [],
            }

            self.db.execute(
                "DELETE FROM transaction_categories WHERE transaction_id = ?",
                (transaction_id,)
            )
            self.db.execute(
                "DELETE FROM transactions WHERE id = ? AND user_id = ?",
                (transaction_id, user_id)
            )

            logger.info(f"Удалена транзакция {transaction_id}")
            return {
                "status": "deleted",
                "deleted_ids": [transaction_id],
                "deleted_transactions": [deleted_snapshot]
            }

        query = """
            SELECT
                t.id, t.name, t.description, t.amount, t.quantity, t.datetime,
                GROUP_CONCAT(c.id, ',') as category_ids_raw,
                GROUP_CONCAT(c.name, ', ') as categories_raw
            FROM transactions t
            LEFT JOIN transaction_categories tc ON t.id = tc.transaction_id
            LEFT JOIN categories c ON tc.category_id = c.id
        """
        clauses, args = self._build_transaction_filter_sql(user_id, params, alias="t")
        query += " WHERE " + " AND ".join(clauses)
        query += " GROUP BY t.id"

        rows = self.db.execute(query, tuple(args), fetch=True)
        if not rows:
            return {
                "status": "deleted",
                "deleted_ids": [],
                "deleted_transactions": []
            }

        deleted_transactions = []
        deleted_ids = []
        for row in rows:
            deleted_ids.append(row["id"])
            deleted_transactions.append({
                "id": row["id"],
                "name": row["name"],
                "description": row["description"],
                "amount": row["amount"],
                "quantity": row["quantity"],
                "datetime": row["datetime"],
                "category_ids": [int(x) for x in row["category_ids_raw"].split(",") if x.strip()] if row.get("category_ids_raw") else [],
                "categories": [x.strip() for x in row["categories_raw"].split(",") if x.strip()] if row.get("categories_raw") else [],
            })

        placeholders = ",".join(["?"] * len(deleted_ids))
        self.db.execute(
            f"DELETE FROM transaction_categories WHERE transaction_id IN ({placeholders})",
            tuple(deleted_ids)
        )
        self.db.execute(
            f"DELETE FROM transactions WHERE id IN ({placeholders}) AND user_id = ?",
            tuple(deleted_ids + [user_id])
        )

        logger.info(f"Удалены транзакции: {deleted_ids}")
        return {
            "status": "deleted",
            "deleted_ids": deleted_ids,
            "deleted_transactions": deleted_transactions
        }

    # ========== КАТЕГОРИИ ==========

    def list_categories(self, telegram_id: int, params: Dict[str, Any]) -> List[Dict]:
        user_id = self._get_user_id(telegram_id)
        return self.db.execute(
            "SELECT id, name FROM categories WHERE user_id = ? ORDER BY name",
            (user_id,),
            fetch=True
        )

    def add_category(self, telegram_id: int, params: Dict[str, Any]) -> Dict:
        user_id = self._get_user_id(telegram_id)
        name = self._pick_param(params, "name")
        if not name:
            raise ValueError("Не указано название категории")

        name = self._normalize_label(name)

        exists = self.db.execute(
            "SELECT id FROM categories WHERE user_id = ? AND name = ?",
            (user_id, name),
            fetch=True
        )
        if exists:
            raise ValueError(f"Категория '{name}' уже существует")

        category_id = self.db.execute(
            "INSERT INTO categories (user_id, name) VALUES (?, ?)",
            (user_id, name)
        )
        logger.info(f"Добавлена категория '{name}'")
        return {
            "status": "created",
            "category": {
                "id": category_id,
                "name": name
            }
        }

    def delete_category(self, telegram_id: int, params: Dict[str, Any]) -> Dict:
        user_id = self._get_user_id(telegram_id)
        category_id = self._pick_param(params, "category_id", "id")
        name = self._pick_param(params, "name")

        if category_id is not None:
            rows = self.db.execute(
                "SELECT id, name FROM categories WHERE id = ? AND user_id = ?",
                (category_id, user_id),
                fetch=True
            )
            if not rows:
                raise ValueError("Категория не найдена")
            category = rows[0]

        elif name:
            name = self._normalize_label(name)
            rows = self.db.execute(
                "SELECT id, name FROM categories WHERE user_id = ? AND name = ?",
                (user_id, name),
                fetch=True
            )
            if not rows:
                raise ValueError("Категория не найдена")
            category = rows[0]

        else:
            raise ValueError("Не указано название или id категории")

        cat_id = category["id"]

        # 👉 1. Удаляем связи
        self.db.execute(
            "DELETE FROM transaction_categories WHERE category_id = ?",
            (cat_id,)
        )

        # 👉 2. Удаляем саму категорию
        self.db.execute(
            "DELETE FROM categories WHERE id = ? AND user_id = ?",
            (cat_id, user_id)
        )

        logger.info(f"Удалена категория '{category['name']}' (id={cat_id})")

        return {
            "status": "deleted",
            "category": {
                "id": cat_id,
                "name": category["name"]
            }
        }

    def rename_category(self, telegram_id: int, params: Dict[str, Any]) -> Dict:
        user_id = self._get_user_id(telegram_id)
        category_id = self._pick_param(params, "category_id", "id")
        old_name = self._pick_param(params, "old_name", "name")
        new_name = self._pick_param(params, "new_name")

        if not new_name:
            raise ValueError("Не указано новое название категории")

        new_name = self._normalize_label(new_name)

        if category_id is not None:
            rows = self.db.execute(
                "SELECT id, name FROM categories WHERE id = ? AND user_id = ?",
                (category_id, user_id),
                fetch=True
            )
            if not rows:
                raise ValueError("Категория не найдена")

            old_name_value = rows[0]["name"]
            self.db.execute(
                "UPDATE categories SET name = ? WHERE id = ? AND user_id = ?",
                (new_name, category_id, user_id)
            )
            logger.info(f"Категория переименована: '{old_name_value}' -> '{new_name}'")
            return {
                "status": "renamed",
                "category": {
                    "id": category_id,
                    "old_name": old_name_value,
                    "new_name": new_name
                }
            }

        if not old_name:
            raise ValueError("Укажите old_name/new_name или category_id/new_name")

        old_name = self._normalize_label(old_name)
        self.db.execute(
            "UPDATE categories SET name = ? WHERE user_id = ? AND name = ?",
            (new_name, user_id, old_name)
        )
        logger.info(f"Категория переименована: '{old_name}' -> '{new_name}'")
        return {
            "status": "renamed",
            "category": {
                "old_name": old_name,
                "new_name": new_name
            }
        }

    def merge_categories(self, telegram_id: int, params: Dict[str, Any]) -> Dict:
        user_id = self._get_user_id(telegram_id)
        source_id = self._pick_param(params, "source_category_id", "source_id")
        target_id = self._pick_param(params, "target_category_id", "target_id")

        if source_id is None or target_id is None:
            raise ValueError("Укажите source_category_id и target_category_id")

        src_rows = self.db.execute(
            "SELECT id, name FROM categories WHERE id = ? AND user_id = ?",
            (source_id, user_id),
            fetch=True
        )
        tgt_rows = self.db.execute(
            "SELECT id, name FROM categories WHERE id = ? AND user_id = ?",
            (target_id, user_id),
            fetch=True
        )
        if not src_rows:
            raise ValueError(f"Категория с id={source_id} не найдена")
        if not tgt_rows:
            raise ValueError(f"Категория с id={target_id} не найдена")

        self.db.execute(
            "UPDATE transaction_categories SET category_id = ? WHERE category_id = ?",
            (target_id, source_id)
        )
        self.db.execute(
            "DELETE FROM categories WHERE id = ? AND user_id = ?",
            (source_id, user_id)
        )
        logger.info(f"Объединение категорий: '{src_rows[0]['name']}' -> '{tgt_rows[0]['name']}'")
        return {
            "status": "merged",
            "source_category": {
                "id": src_rows[0]["id"],
                "name": src_rows[0]["name"]
            },
            "target_category": {
                "id": tgt_rows[0]["id"],
                "name": tgt_rows[0]["name"]
            }
        }

    # ========== ОТЧЁТЫ ==========

    def get_spending_summary(self, telegram_id: int, params: Dict[str, Any]) -> Dict:
        user_id = self._get_user_id(telegram_id)
        date_from = self._pick_param(params, "date_from")
        date_to = self._pick_param(params, "date_to")
        period = self._pick_param(params, "period")

        if period:
            now = datetime.now()
            if period == "day":
                date_from = date_to = now.strftime("%Y-%m-%d")
            elif period == "week":
                start = now - timedelta(days=now.weekday())
                date_from = start.strftime("%Y-%m-%d")
                date_to = now.strftime("%Y-%m-%d")
            elif period == "month":
                date_from = now.replace(day=1).strftime("%Y-%m-%d")
                date_to = now.strftime("%Y-%m-%d")
            elif period == "year":
                date_from = now.replace(month=1, day=1).strftime("%Y-%m-%d")
                date_to = now.strftime("%Y-%m-%d")

        query = """
            SELECT
                CASE WHEN t.amount >= 0 THEN 'income' ELSE 'expense' END as type,
                COALESCE(c.name, 'Без категории') as category,
                SUM(t.amount) as total
            FROM transactions t
            LEFT JOIN transaction_categories tc ON t.id = tc.transaction_id
            LEFT JOIN categories c ON tc.category_id = c.id
            WHERE t.user_id = ?
        """
        args = [user_id]

        if date_from is not None:
            query += " AND t.datetime >= ?"
            args.append(self._parse_datetime(date_from, end_of_day=False))
        if date_to is not None:
            query += " AND t.datetime <= ?"
            args.append(self._parse_datetime(date_to, end_of_day=True))

        query += " GROUP BY type, category"

        rows = self.db.execute(query, tuple(args), fetch=True)
        result = {
            "incomes": {"total": 0, "by_category": {}},
            "expenses": {"total": 0, "by_category": {}}
        }

        for r in rows:
            typ = r["type"]
            cat = r["category"]
            total = r["total"]
            if typ == "income":
                result["incomes"]["total"] += total
                result["incomes"]["by_category"][cat] = result["incomes"]["by_category"].get(cat, 0) + total
            else:
                result["expenses"]["total"] += total
                result["expenses"]["by_category"][cat] = result["expenses"]["by_category"].get(cat, 0) + total

        return {
            "status": "ok",
            "date_from": date_from,
            "date_to": date_to,
            "summary": result
        }

    def get_category_breakdown(self, telegram_id: int, params: Dict[str, Any]) -> Dict:
        user_id = self._get_user_id(telegram_id)
        category_id = self._pick_param(params, "category_id", "id")
        category_name = self._pick_param(params, "category", "name")

        if category_id is None and not category_name:
            # Если категория не указана — возвращаем агрегат по ВСЕМ категориям
            query = """
                SELECT
                    t.id,
                    t.name,
                    t.description,
                    t.amount,
                    t.quantity,
                    t.datetime
                FROM transactions t
                WHERE t.user_id = ?
            """
            args = [user_id]

            date_from = self._pick_param(params, "date_from")
            if date_from is not None:
                query += " AND t.datetime >= ?"
                args.append(self._parse_datetime(date_from, end_of_day=False))

            date_to = self._pick_param(params, "date_to")
            if date_to is not None:
                query += " AND t.datetime <= ?"
                args.append(self._parse_datetime(date_to, end_of_day=True))

            txns = self.db.execute(query, tuple(args), fetch=True)

            total_income = sum(t["amount"] for t in txns if t["amount"] > 0)
            total_expense = sum(t["amount"] for t in txns if t["amount"] < 0)

            return {
                "status": "ok",
                "category": None,
                "transactions": txns,
                "total_income": total_income,
                "total_expense": total_expense
            }

        category_row = None
        if category_id is None:
            category_name = self._normalize_label(category_name)
            rows = self.db.execute(
                "SELECT id, name FROM categories WHERE user_id = ? AND name = ?",
                (user_id, category_name),
                fetch=True
            )
            if not rows:
                return {
                    "status": "ok",
                    "category": None,
                    "transactions": [],
                    "total_income": 0,
                    "total_expense": 0
                }
            category_row = rows[0]
            category_id = category_row["id"]
        else:
            rows = self.db.execute(
                "SELECT id, name FROM categories WHERE id = ? AND user_id = ?",
                (category_id, user_id),
                fetch=True
            )
            if not rows:
                raise ValueError("Категория не найдена")
            category_row = rows[0]

        query = """
            SELECT
                t.id,
                t.name,
                t.description,
                t.amount,
                t.quantity,
                t.datetime
            FROM transactions t
            JOIN transaction_categories tc ON t.id = tc.transaction_id
            WHERE t.user_id = ? AND tc.category_id = ?
        """
        args = [user_id, category_id]

        date_from = self._pick_param(params, "date_from")
        if date_from is not None:
            query += " AND t.datetime >= ?"
            args.append(self._parse_datetime(date_from, end_of_day=False))
        date_to = self._pick_param(params, "date_to")
        if date_to is not None:
            query += " AND t.datetime <= ?"
            args.append(self._parse_datetime(date_to, end_of_day=True))
        amount_from = self._pick_param(params, "amount_from")
        if amount_from is not None:
            query += " AND t.amount >= ?"
            args.append(amount_from)
        amount_to = self._pick_param(params, "amount_to")
        if amount_to is not None:
            query += " AND t.amount <= ?"
            args.append(amount_to)

        txns = self.db.execute(query, tuple(args), fetch=True)
        total_income = sum(t["amount"] for t in txns if t["amount"] > 0)
        total_expense = sum(t["amount"] for t in txns if t["amount"] < 0)

        return {
            "status": "ok",
            "category": category_row,
            "transactions": txns,
            "total_income": total_income,
            "total_expense": total_expense
        }

    def get_balance(self, telegram_id: int, params: Dict[str, Any]) -> Dict:
        user_id = self._get_user_id(telegram_id)
        query = "SELECT COALESCE(SUM(amount), 0) as balance FROM transactions WHERE user_id = ?"
        args = [user_id]

        date_from = self._pick_param(params, "date_from")
        if date_from is not None:
            query += " AND datetime >= ?"
            args.append(self._parse_datetime(date_from, end_of_day=False))

        date_to = self._pick_param(params, "date_to")
        if date_to is not None:
            query += " AND datetime <= ?"
            args.append(self._parse_datetime(date_to, end_of_day=True))

        rows = self.db.execute(query, tuple(args), fetch=True)
        balance = rows[0]["balance"] if rows else 0
        return {
            "status": "ok",
            "balance": balance,
            "date_from": date_from,
            "date_to": date_to
        }