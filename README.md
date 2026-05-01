# SEO Sitemap Crawler — Exact Sitemap Mode

Эта версия по умолчанию берет URL строго из тех sitemap-файлов, которые вставлены в поле.

## Главное изменение

По умолчанию выключен обход вложенных sitemap.

То есть если вставить:

https://art-lichnost.ru/page-sitemap.xml

краулер возьмет только URL из этого файла.

Если вставить sitemap index:

https://site.ru/sitemap.xml

он НЕ пойдет во все вложенные sitemap, пока вы вручную не включите галочку:

"Если это sitemap index — идти во вложенные sitemap"

## Файлы для загрузки в GitHub

- app.py
- requirements.txt
- runtime.txt

После замены:

1. Push в GitHub.
2. Streamlit Cloud → Manage app.
3. Clear cache.
4. Reboot app.
