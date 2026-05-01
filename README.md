# SEO Sitemap Crawler — Strict URL Mode

Жесткая версия.

## Главное

Краулер НЕ собирает ссылки со страниц сайта.

Он делает только это:

1. Загружает sitemap, который вы вставили.
2. Показывает список URL, найденных именно в этом sitemap.
3. Обходит только эти URL.
4. Собирает Title, Description, index/noindex, canonical, H1.
5. Экспортирует XLSX / CSV.

## Важное отличие

Если вставить sitemap index, например:

https://site.ru/sitemap.xml

и НЕ включить режим "Развернуть sitemap index", краулер НЕ пойдет во вложенные sitemap.

Если вставить конкретный page-sitemap:

https://site.ru/page-sitemap.xml

он возьмет только URL из него.

## Файлы для GitHub

Замените в корне репозитория:

- app.py
- requirements.txt
- runtime.txt

После замены:

1. Push в GitHub.
2. Streamlit Cloud → Manage app.
3. Clear cache.
4. Reboot app.
