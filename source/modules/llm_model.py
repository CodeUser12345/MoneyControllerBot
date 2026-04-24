import requests
import logging
from source.core.config import Config


logger = logging.getLogger(__name__)


class LLMModel:
    def __init__(self, config: Config):
        self.api_key = config.get('openrouter.api_key')
        if not self.api_key:
            logger.error("Не указан API ключ OpenRouter в конфиге")
        self.model = config.get('openrouter.model')
        self.base_url = config.get('openrouter.base_url')
        self.timeout = config.get('openrouter.timeout', 30)
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        self.messages = Config(config.get("bot.messages_path"))

    def query(self, user_message: str, system_message: str = "You are a helpful assistant.") -> str:
        """
        Отправляет сообщение пользователя в модель и возвращает ответ.
        """
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_message},
                {"role": "user", "content": user_message}
            ]
        }
        try:
            response = requests.post(
                self.base_url,
                headers=self.headers,
                json=payload,
                timeout=self.timeout
            )
            response.raise_for_status()
            data = response.json()
            # Обработка ответа OpenRouter
            if "choices" in data and len(data["choices"]) > 0:
                content = data["choices"][0]["message"]["content"]
                return content.strip()
            else:
                logger.error(f"Неожиданный ответ API: {data}")
                return self.messages.get("error_llm_1")
        except requests.exceptions.Timeout:
            logger.error("Таймаут при запросе к OpenRouter")
            return self.messages.get("error_llm_2")
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка при запросе к OpenRouter: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Тело ответа: {e.response.text}")
            return self.messages.get("error_llm_3")
        except Exception as error:
            logger.exception(f"Непредвиденная ошибка в LLMModel: {error}")
            return self.messages.get("error_llm_4")