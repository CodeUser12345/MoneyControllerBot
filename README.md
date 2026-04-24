# MoneyControllerBot

Telegram-бот для учёта доходов и расходов с использованием LLM.

---

## Установка и запуск

### 1. Клонирование проекта

```bash
git clone https://github.com/CodeUser12345/MoneyControllerBot
cd MoneyControllerBot
```

---

### 2. Создание и активация виртуального окружения

```bash
python -m venv .venv
```

**Windows (PowerShell):**

```bash
.venv\Scripts\activate
```

---

### 3. Обновление pip

```bash
python -m pip install --upgrade pip
```

---

### 4. Установка зависимостей

```bash
pip install -r requirements.txt
```

---

## Настройка распознавания голоса (Vosk)

1. Перейти на сайт:
   https://alphacephei.com/vosk/models

2. Скачать модель:
   **vosk-model-ru-0.22**

3. Распаковать в папку:

```text
data/model
```

---

## Установка FFmpeg

1. Скачать сборку:
   https://www.gyan.dev/ffmpeg/builds/ffmpeg-git-full.7z

2. Распаковать архив

3. Добавить путь к папке `bin` в переменную окружения **PATH**

Пример:

```text
C:\ffmpeg\bin
```

---

## Настройка конфигурации

В файле конфигурации (например `config.yaml`) необходимо указать:

### Telegram

```yaml
bot:
  token: YOUR_TELEGRAM_TOKEN
```

---

## ▶Запуск бота

```bash
python main.py
```

---

## Возможности

* Добавление расходов и доходов через текст
* Голосовой ввод (Vosk)
* Обработка изображений (QR-коды чеков)
* Интеграция с LLM
* Работа с категориями и транзакциями

---

## Генерация документации

```bash
cd docs
.\make.bat html
```

После этого документация будет доступна:

```text
docs/build/html/index.html
```

---

## ⚠Примечания

* Требуется установленный FFmpeg
* Модель Vosk должна быть распакована вручную
* API чеков может требовать токен и иметь ограничения
* Бот работает только для разрешённых пользователей (таблица users в БД)

---
