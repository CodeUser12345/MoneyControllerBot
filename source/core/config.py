import yaml
import logging


logger = logging.getLogger(__name__)


class Config:
    """
    Класс для работы с конфигурацией.

    Загружает YAML файл и предоставляет доступ к параметрам
    через точечную нотацию (например: 'bot.token').
    """
    def __init__(self, config_path):
        self.config_path = config_path
        self._data = {}
        self.load()

    def load(self):
        """
       Загружает конфигурацию из файла.
       """
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                self._data = yaml.safe_load(f)
            logger.info(f"Конфигурация загружена из {self.config_path}")
        except Exception as e:
            logger.error(f"Ошибка загрузки конфигурации: {e}")
            self._data = {}

    def get(self, key, default=None):
        """
        Получает значение по ключу.

        :param key: строка с точечной нотацией
        :param default: значение по умолчанию
        :return: значение или default
        """
        keys = key.split('.')
        value = self._data
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
                if value is None:
                    return default
            else:
                return default
        return value

    @property
    def raw(self):
        return self._data