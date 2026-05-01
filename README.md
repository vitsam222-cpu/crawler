# SEO Sitemap Crawler — Pages Only

Исправление: краулер берет только URL страниц из sitemap.

## Что исправлено

В WordPress sitemap внутри каждой страницы могут быть image:loc для картинок.

Пример:

<url>
  <loc>https://site.ru/page/</loc>
  <image:image>
    <image:loc>https://site.ru/wp-content/uploads/image.jpg</image:loc>
  </image:image>
</url>

Старая версия брала все loc, включая image:loc.
Эта версия берет только прямой loc внутри url.

## Дополнительно

Отсекаются ассеты:

- jpg
- jpeg
- png
- gif
- webp
- svg
- pdf
- css
- js
- xml
- zip
- rar
- mp4
- webm
- mp3

## Использование

1. Замените app.py, requirements.txt, runtime.txt.
2. Push в GitHub.
3. Streamlit Cloud → Manage app → Clear cache → Reboot app.
