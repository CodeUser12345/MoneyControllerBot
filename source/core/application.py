import logging
from source.core.config import Config
from source.modules.voice_recognition import VoiceRecognizer
from source.modules.tg_bot import TgBot
from source.modules.llm_model import LLMModel
from source.modules.database_manager import DatabaseManager
from source.modules.database_actions import DatabaseActions
from source.modules.dispatcher import ActionDispatcher


logger = logging.getLogger(__name__)


class Application:
    """
    Главный класс приложения.

    Отвечает за:
    - загрузку конфигурации
    - инициализацию всех модулей
    - запуск Telegram-бота
    """
    def __init__(self, config_path: str):
        """
        Инициализирует приложение.

        :param config_path: путь к YAML конфигурации
        """
        self.config = Config(config_path)
        self.voice_recognizer = None
        self.llm_model = None
        self.db_manager = None
        self.db_actions = None
        self.dispatcher = None
        self.bot = None
        self._init_modules()

    def _init_modules(self):
        """
        Инициализирует все основные модули системы:
        - распознавание голоса
        - LLM модель
        - база данных
        - dispatcher
        - Telegram-бот
        """
        # Инициализация распознавателя голоса
        self.voice_recognizer = VoiceRecognizer(self.config)

        # Инициализация LLM модели
        self.llm_model = LLMModel(self.config)

        # Инициализация финансового менеджера БД, инициализация менеджера действий и dispatcher
        self.db_manager = DatabaseManager(self.config)
        self.db_actions = DatabaseActions(self.config, self.db_manager)
        self.dispatcher = ActionDispatcher(self.config, self.db_actions)

        # Инициализация бота
        self.bot = TgBot(self.config, self.voice_recognizer, self.llm_model, self.dispatcher)

    def run(self):
        """Запуск приложения (бота)."""
        self.bot.run()
