# SEO Sitemap Crawler

Готовый онлайн-краулер для проверки SEO-метаданных страниц из sitemap.

## Что собирает

По каждому URL:

- URL
- Status
- Final URL
- Redirect
- Redirect chain
- Title
- Description
- Index/noindex
- Canonical
- H1
- H1 count
- Meta robots
- X-Robots-Tag
- Source sitemap
- Error

## Почему это лучше HTML-файла

HTML-файл в браузере часто падает с `Failed to fetch` из-за CORS.  
Этот вариант запускается на сервере через Python, поэтому CORS не мешает.

## Быстрый локальный запуск

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
streamlit run app.py
```

На Windows:

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
streamlit run app.py
```

## Онлайн-запуск через Streamlit Cloud

1. Создайте репозиторий на GitHub.
2. Загрузите в него файлы:
   - `app.py`
   - `requirements.txt`
3. Откройте Streamlit Cloud.
4. Создайте новое приложение из GitHub-репозитория.
5. Main file path укажите:
   - `app.py`
6. Запустите приложение.

Ключи и переменные окружения не нужны.

## Как пользоваться

1. Вставьте одну или несколько ссылок на sitemap, каждая с новой строки.

Пример:

```text
https://art-lichnost.ru/page-sitemap.xml
https://msk.art-lichnost.ru/page-sitemap.xml
```

2. Выберите лимит URL.
3. Выберите количество параллельных запросов.
4. Нажмите `Запустить краулер`.
5. Скачайте результат в XLSX или CSV.

## Рекомендованные настройки

Для аккуратного обхода:

- Параллельных запросов: `3–5`
- Timeout: `20`
- Пауза после запроса: `0–0.3`

Для своего сайта можно поднять параллельность до `10–20`.

## Логика index/noindex

- Если URL редиректит → `redirect`
- Если HTTP status >= 400 → `not indexable`
- Если найден `noindex` в meta robots или X-Robots-Tag → `noindex`
- Иначе → `index`

## Важно

Если URL из sitemap редиректит, его лучше убрать из sitemap.  
Если noindex-страница есть в sitemap, ее тоже лучше убрать из sitemap.
