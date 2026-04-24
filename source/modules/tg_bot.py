import telebot
import logging
import os
from pydub import AudioSegment
import wave
import json
from datetime import datetime
import cv2
import requests
from urllib.parse import parse_qs
import subprocess
from source.core.config import Config


logger = logging.getLogger(__name__)


class TgBot:
    """
        Основной класс Telegram-бота.

        Отвечает за:
        - обработку сообщений
        - взаимодействие с LLM
        - выполнение действий
        """
    def __init__(self, config: Config, voice_recognizer, llm_model, dispatcher):
        """
        Инициализация Telegram-бота.

        :param config: объект конфигурации
        :param voice_recognizer: модуль распознавания голоса
        :param llm_model: модуль LLM
        :param dispatcher: диспетчер действий
        """
        self.voice_recognizer = voice_recognizer
        self.llm_model = llm_model
        self.dispatcher = dispatcher
        self.token = config.get('bot.token')
        if not self.token:
            logger.error(f"Токен бота не найден в конфигурации")
        self.cache_path = config.get('bot.cache_path')
        self.messages = Config(config.get("bot.messages_path"))
        self.bot = telebot.TeleBot(self.token)
        self.promt_for_db = self._load_promt([config.get("bot.promt_for_db_path"), config.get("bot.promt_for_db_script_path")])
        self.promt_for_answer = self._load_promt([config.get("bot.promt_for_answer_path")])
        self.max_iterations = config.get("bot.max_iterations")
        self._register_handlers()

    def send_message(self, message, text):
        """
        Отправляет сообщение пользователю.

        :param message: объект Telegram сообщения
        :param text: текст ответа
        """
        self.bot.reply_to(message, text, parse_mode='Markdown')

    def _check_user(self, message):
        """
        Проверяет, есть ли у пользователя доступ к боту.
        """
        user_id = message.from_user.id
        if not self.dispatcher.db.is_user_allowed(user_id):
            self.send_message(message, self.messages.get("error_access_1") + self.messages.get("error_access_2") +
                              str(user_id) + self.messages.get("error_access_3") + self.messages.get("error_access_4"))
            logger.error(f"ID: {user_id}. Недостаточно прав, заблокирован запрос.")
            return False
        return True

    def _load_promt(self, paths):
        """
        Загружает текст промтов из файлов.
        """
        text = ""
        for path in paths:
            with open(path, "r", encoding="utf-8") as file:
                text += f"Файл {os.path.basename(path)}:\n" + file.read()
        return text

    def _build_user_message(self, original_text: str, iteration: int, history: list[dict], input_kind: str) -> str:
        """
        Формирует сообщение для LLM с учётом истории.

        :return: строка запроса для модели
        """
        time_now = datetime.now()
        formatted_time_now = time_now.strftime("%Y-%m-%d %H:%M:%S")
        parts = [
            f"Текущее время: {formatted_time_now}",
            f"Тип входных данных: {input_kind}",
            f"Исходный запрос пользователя: {original_text}",
            f"Итерация: {iteration}/{self.max_iterations}",
        ]

        if history:
            parts.append("История предыдущих итераций:")
            for item in history:
                parts.append(f"- Итерация {item['iteration']}")
                parts.append(f"- Ответ модели: {item['db_response']}")
                parts.append(
                    "- Выполненные действия: "
                    + json.dumps(item["results"], ensure_ascii=False)
                )
        return "\n".join(parts)

    def _generate_answer(self, message, text, input_kind="text"):
        """
            Генерирует ответ пользователю через LLM.

            :param message: объект сообщения Telegram
            :param text: текст запроса
            :param input_kind: тип входных данных (text, voice, qr)
            :return: None
            """
        db_history = []

        try:
            for iteration in range(1, self.max_iterations + 1):
                db_user_message = self._build_user_message(text, iteration, db_history, input_kind)
                logger.info(f"ID: {message.from_user.id}. DB-итерация {iteration}, запрос: {db_user_message}")
                db_response = self.llm_model.query(db_user_message, system_message=self.promt_for_db)
                logger.info(f"ID: {message.from_user.id}. DB-итерация {iteration}, ответ: {db_response}")

                try:
                    data = json.loads(db_response)
                except json.JSONDecodeError:
                    logger.error(f"ID: {message.from_user.id}. DB-ответ не JSON: {db_response}")
                    self.send_message(message, self.messages.get("error_parse_message"))
                    return

                actions = data.get("actions", [])
                if not actions:
                    break

                results = self.dispatcher.execute_actions(actions, message.from_user.id)

                db_history.append({
                    "iteration": iteration,
                    "db_response": db_response,
                    "results": results,
                })

            final_user_message = self._build_user_message(text, -1, db_history, input_kind)
            final_answer = self.llm_model.query(final_user_message, system_message=self.promt_for_answer)
            self.send_message(message, final_answer)

        except Exception as error:
            logger.exception(error)
            self.send_message(message, self.messages.get("error_message"))

    def _register_handlers(self):
        """
        Регистрирует обработчики сообщений Telegram.
        """
        @self.bot.message_handler(commands=['start', 'help'])
        def send_welcome(message):
            if not self._check_user(message):
                return

            self.bot.send_chat_action(message.chat.id, 'typing')

            self.send_message(message, self.messages.get("start"))

        @self.bot.message_handler(func=lambda message: True, content_types=['text'])
        def echo_text(message):
            try:
                if not self._check_user(message):
                    return

                self.bot.send_chat_action(message.chat.id, 'typing')

                self._generate_answer(message, message.text, input_kind="text")
            except Exception as error:
                logger.exception(error)
                self.send_message(message, self.messages.get("error_message"))

        @self.bot.message_handler(content_types=['voice'])
        def handle_voice(message):
            try:
                if not self._check_user(message):
                    return

                self.bot.send_chat_action(message.chat.id, 'typing')

                logger.info(f"ID: {message.from_user.id}. Обработка голосового сообщения.")
                text = self._process_voice(message)
                logger.info(f"ID: {message.from_user.id}. Обработано голосовое сообщение.")

                self._generate_answer(message, text, input_kind="voice")
            except Exception as error:
                logger.exception(error)
                self.send_message(message, self.messages.get("error_message"))

        @self.bot.message_handler(content_types=['photo'])
        def handle_photo(message):
            try:
                if not self._check_user(message):
                    return

                self.bot.send_chat_action(message.chat.id, 'typing')

                logger.info(f"ID: {message.from_user.id}. Обработка изображения.")
                extracted_text, input_kind = self._process_photo(message)
                logger.info(f"ID: {message.from_user.id}. Обработано изображение. Тип: {input_kind}")

                caption = (message.caption or "").strip()
                if caption and extracted_text:
                    text = f"{caption}\n\n[Текст из изображения]\n{extracted_text}"
                    input_kind = f"{input_kind} + text"
                elif caption:
                    text = caption
                    input_kind = "image + text"
                else:
                    text = extracted_text

                if not text.strip():
                    self.send_message(message, "Не удалось распознать QR/штрих-код на изображении.")
                    return

                self._generate_answer(message, text, input_kind=input_kind)
            except Exception as error:
                logger.exception(error)
                self.send_message(message, self.messages.get("error_message"))

    def _process_voice(self, message):
        """
        Обрабатывает изображение и пытается извлечь QR-код.

        :return: (текст, тип входа)
        """
        # Проверка ffmpeg
        ffmpeg_available = self._check_ffmpeg()
        if not ffmpeg_available:
            self.send_message(message, self.messages.get("error_ffmpeg"))
            return

        ogg_path = self.cache_path + "_" + str(message.from_user.id) + 'voice.ogg'
        wav_path = self.cache_path + "_" + str(message.from_user.id) + 'voice.wav'
        try:
            # Скачиваем файл
            file_info = self.bot.get_file(message.voice.file_id)
            file = self.bot.download_file(file_info.file_path)
            logger.info(f"ID: {message.from_user.id}. Скачано {len(file)} байт")

            with open(ogg_path, 'wb') as f:
                f.write(file)

            # Конвертация в WAV
            audio = AudioSegment.from_ogg(ogg_path)
            audio = audio.set_frame_rate(16000).set_channels(1).set_sample_width(2)
            audio.export(wav_path, format='wav')
            logger.info(f"ID: {message.from_user.id}. Конвертация в WAV завершена.")

            # Проверка длительности (опционально)
            with wave.open(wav_path, 'rb') as wf:
                frames = wf.getnframes()
                rate = wf.getframerate()
                duration = frames / float(rate)
                if duration < 0.5:
                    self.send_message(message, self.messages.get("error_short_gs_message"))
                    return

            # Распознавание
            text = self.voice_recognizer.recognize(wav_path)

            # Удаляем временные файлы
            for path in [ogg_path, wav_path]:
                if os.path.exists(path):
                    try:
                        os.remove(path)
                        logger.info(f"ID: {message.from_user.id}. Удалён {path}")
                    except Exception as error:
                        logger.error(f"ID: {message.from_user.id}. Не удалось удалить {path}: {error}")

            logger.info(f"ID: {message.from_user.id}. Vosk распознал: {text}")
            return text

        except Exception as error:
            self.send_message(message, self.messages.get("error_voice") + error)
            logger.exception(error)

    def _check_ffmpeg(self):
        """Проверяет доступность ffmpeg в системе."""
        try:
            subprocess.run(['ffmpeg', '-version'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            return True
        except FileNotFoundError:
            return False

    def _process_photo(self, message):
        photo = message.photo[-1]
        file_info = self.bot.get_file(photo.file_id)
        file_data = self.bot.download_file(file_info.file_path)

        tmp_path = self.cache_path + "_" + str(message.from_user.id) + "_photo.jpg"

        try:
            with open(tmp_path, "wb") as f:
                f.write(file_data)

            img = cv2.imread(tmp_path)

            detector = cv2.QRCodeDetector()
            data, points, _ = detector.detectAndDecode(img)

            if data:
                return data.strip(), "qr"

            return "", "image"

        finally:
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass

    def run(self):
        """Запуск бота (бесконечный polling)."""
        logger.info("Бот запущен")
        self.bot.infinity_polling()
