import wave
import json
import logging
import os
from vosk import Model, KaldiRecognizer
from source.core.config import Config


logger = logging.getLogger(__name__)


class VoiceRecognizer:
    def __init__(self, config: Config):
        self.model_path = config.get('vosk.model_path')
        self.model = None
        self.load_model()

    def load_model(self):
        """Загружает модель Vosk. Вызывается при инициализации."""
        if not os.path.exists(self.model_path):
            logger.error(f"Модель Vosk не найдена по пути: {self.model_path}")
        self.model = Model(self.model_path)
        logger.info("Модель Vosk успешно загружена")

    def recognize(self, wav_path: str) -> str:
        """
        Распознаёт речь из WAV-файла (моно, 16 кГц, 16 бит).
        Возвращает распознанный текст или пустую строку.
        """
        if not self.model:
            logger.error(f"Модель не загружена")

        wf = wave.open(wav_path, "rb")
        try:
            rec = KaldiRecognizer(self.model, wf.getframerate())
            rec.SetWords(True)

            results = []
            while True:
                data = wf.readframes(4000)
                if len(data) == 0:
                    break
                if rec.AcceptWaveform(data):
                    res = json.loads(rec.Result())
                    results.append(res.get("text", ""))

            final_res = json.loads(rec.FinalResult())
            results.append(final_res.get("text", ""))

            text = " ".join(results).strip()
            return text
        finally:
            wf.close()