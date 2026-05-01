import asyncio
import gzip
import io
import re
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Iterable
from urllib.parse import urlparse

import httpx
import pandas as pd
import streamlit as st
from bs4 import BeautifulSoup


APP_TITLE = "SEO Sitemap Crawler"

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (compatible; SEO-Sitemap-Crawler/2.1; "
    "+https://example.com/seo-crawler)"
)

ASSET_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".avif", ".ico",
    ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
    ".css", ".js", ".json", ".xml", ".txt",
    ".zip", ".rar", ".7z", ".tar", ".gz",
    ".mp4", ".webm", ".mov", ".avi", ".mp3", ".wav", ".ogg",
    ".woff", ".woff2", ".ttf", ".otf", ".eot",
}

RESULT_COLUMNS = [
    "URL",
    "Status",
    "Final URL",
    "Redirect",
    "Redirect chain",
    "Title",
    "Description",
    "Index/noindex",
    "Canonical",
    "H1",
    "H1 count",
    "Meta robots",
    "X-Robots-Tag",
    "Source sitemap",
    "Error",
]


@dataclass
class CrawlSettings:
    timeout: float
    concurrency: int
    max_pages: int
    user_agent: str
    delay_between_requests: float
    keep_assets: bool


def normalize_text(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", value.replace("\xa0", " ")).strip()


def unique_non_empty(values: Iterable[str]) -> list[str]:
    seen = set()
    out = []
    for value in values:
        item = normalize_text(value)
        if not item:
            continue
        key = item.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def sheet_name_from_domain(domain: str) -> str:
    clean = re.sub(r"[\[\]\:\*\?\/\\]", "_", domain)
    return clean[:31] or "Sheet"


def decompress_if_needed(content: bytes) -> bytes:
    if content[:2] == b"\x1f\x8b":
        return gzip.decompress(content)
    return content


def strip_namespace(tag: str) -> str:
    return tag.split("}", 1)[-1] if "}" in tag else tag


def is_asset_url(url: str) -> bool:
    path = urlparse(url).path.lower()
    return any(path.endswith(ext) for ext in ASSET_EXTENSIONS)


def get_direct_child_text_by_localname(parent, child_localname: str) -> str:
    for child in list(parent):
        if strip_namespace(child.tag).lower() == child_localname.lower():
            return normalize_text(child.text or "")
    return ""


def parse_sitemap_xml(xml_bytes: bytes) -> tuple[str, list[str]]:
    """
    Correct sitemap parser:
    - urlset: only direct /urlset/url/loc
    - sitemapindex: only direct /sitemapindex/sitemap/loc
    This intentionally ignores image:loc, video:loc, news:loc etc.
    """
    xml_bytes = decompress_if_needed(xml_bytes)

    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        soup = BeautifulSoup(xml_bytes, "html.parser")
        # Fallback, but still try to take loc directly inside url/sitemap.
        urls = []
        for url_tag in soup.find_all("url"):
            loc = url_tag.find("loc", recursive=False)
            if loc:
                text = normalize_text(loc.get_text())
                if text:
                    urls.append(text)
        if urls:
            return "urlset", urls

        sitemaps = []
        for sm_tag in soup.find_all("sitemap"):
            loc = sm_tag.find("loc", recursive=False)
            if loc:
                text = normalize_text(loc.get_text())
                if text:
                    sitemaps.append(text)
        if sitemaps:
            return "sitemapindex", sitemaps

        return "unknown", []

    root_name = strip_namespace(root.tag).lower()

    if root_name == "urlset":
        locs = []
        for url_node in list(root):
            if strip_namespace(url_node.tag).lower() != "url":
                continue
            loc = get_direct_child_text_by_localname(url_node, "loc")
            if loc:
                locs.append(loc)
        return "urlset", locs

    if root_name == "sitemapindex":
        locs = []
        for sitemap_node in list(root):
            if strip_namespace(sitemap_node.tag).lower() != "sitemap":
                continue
            loc = get_direct_child_text_by_localname(sitemap_node, "loc")
            if loc:
                locs.append(loc)
        return "sitemapindex", locs

    return "unknown", []


async def fetch_bytes(client: httpx.AsyncClient, url: str) -> bytes:
    response = await client.get(url, follow_redirects=True)
    response.raise_for_status()
    return response.content


async def extract_urls_from_one_sitemap(
    client: httpx.AsyncClient,
    sitemap_url: str,
    expand_sitemap_index: bool,
    keep_assets: bool,
    log_box=None,
) -> list[dict]:
    if log_box:
        log_box.info(f"Читаю sitemap: {sitemap_url}")

    content = await fetch_bytes(client, sitemap_url)
    sitemap_type, locs = parse_sitemap_xml(content)

    def make_pages(source_sitemap: str, loc_list: list[str]) -> list[dict]:
        pages = []
        skipped_assets = 0
        for loc in loc_list:
            if not keep_assets and is_asset_url(loc):
                skipped_assets += 1
                continue
            pages.append({"URL": loc, "Source sitemap": source_sitemap})
        if log_box and skipped_assets:
            log_box.info(f"Пропущено ассетов из {source_sitemap}: {skipped_assets}")
        return pages

    if sitemap_type == "urlset":
        return make_pages(sitemap_url, locs)

    if sitemap_type == "sitemapindex":
        if not expand_sitemap_index:
            if log_box:
                log_box.warning(
                    f"{sitemap_url} — это sitemap index. Вложенные sitemap НЕ разворачиваются. "
                    "Вставьте конкретный page-sitemap.xml или включите режим разворота."
                )
            return []

        out = []
        for child_sitemap in locs:
            try:
                if log_box:
                    log_box.info(f"Читаю вложенный sitemap: {child_sitemap}")

                child_content = await fetch_bytes(client, child_sitemap)
                child_type, child_locs = parse_sitemap_xml(child_content)

                if child_type == "urlset":
                    out.extend(make_pages(child_sitemap, child_locs))
                else:
                    if log_box:
                        log_box.warning(f"Пропущен вложенный sitemap не типа urlset: {child_sitemap}")
            except Exception as exc:
                if log_box:
                    log_box.error(f"Ошибка вложенного sitemap {child_sitemap}: {exc}")

        return out

    if log_box:
        log_box.error(f"Неизвестный формат sitemap: {sitemap_url}")
    return []


async def collect_urls(
    sitemap_urls: list[str],
    settings: CrawlSettings,
    expand_sitemap_index: bool,
    log_box=None,
) -> list[dict]:
    headers = {"User-Agent": settings.user_agent}
    all_pages = []

    async with httpx.AsyncClient(
        headers=headers,
        timeout=settings.timeout,
        verify=True,
        follow_redirects=True,
    ) as client:
        for sitemap_url in sitemap_urls:
            try:
                pages = await extract_urls_from_one_sitemap(
                    client=client,
                    sitemap_url=sitemap_url,
                    expand_sitemap_index=expand_sitemap_index,
                    keep_assets=settings.keep_assets,
                    log_box=log_box,
                )
                all_pages.extend(pages)
            except Exception as exc:
                if log_box:
                    log_box.error(f"Ошибка sitemap {sitemap_url}: {exc}")

    seen = set()
    clean = []
    for item in all_pages:
        url = item["URL"]
        if url in seen:
            continue
        seen.add(url)
        clean.append(item)

    return clean[: settings.max_pages]


def meta_by_name(soup: BeautifulSoup, names: set[str]) -> str:
    values = []
    for meta in soup.find_all("meta"):
        name = (meta.get("name") or "").strip().lower()
        if name in names:
            values.append(meta.get("content") or "")
    return normalize_text(" ".join(unique_non_empty(values)))


def get_description(soup: BeautifulSoup) -> str:
    return meta_by_name(soup, {"description"})


def get_canonical(soup: BeautifulSoup) -> str:
    for link in soup.find_all("link"):
        rel = link.get("rel")
        if isinstance(rel, list):
            rels = [str(item).lower() for item in rel]
        else:
            rels = str(rel or "").lower().split()
        if "canonical" in rels:
            return normalize_text(link.get("href") or "")
    return ""


HIDDEN_CLASS_RE = re.compile(
    r"(^|\s)(hidden|visually-hidden|sr-only|d-none|display-none|t-hidden|uc-hidden)(\s|$)",
    re.I,
)


def is_hidden_tag(tag) -> bool:
    current = tag
    while current is not None and getattr(current, "name", None):
        if current.has_attr("hidden"):
            return True

        aria_hidden = str(current.get("aria-hidden", "")).strip().lower()
        if aria_hidden == "true":
            return True

        style = str(current.get("style", "")).replace(" ", "").lower()
        if "display:none" in style or "visibility:hidden" in style:
            return True

        classes_raw = current.get("class", "")
        classes = " ".join(classes_raw) if isinstance(classes_raw, list) else str(classes_raw)
        if HIDDEN_CLASS_RE.search(classes):
            return True

        current = current.parent

    return False


def get_h1(soup: BeautifulSoup) -> tuple[str, int]:
    all_h1_tags = soup.find_all("h1")

    visible_values = [
        tag.get_text(" ", strip=True)
        for tag in all_h1_tags
        if not is_hidden_tag(tag)
    ]
    visible_unique = unique_non_empty(visible_values)

    if visible_unique:
        return " | ".join(visible_unique), len(visible_unique)

    fallback_unique = unique_non_empty([tag.get_text(" ", strip=True) for tag in all_h1_tags])
    return " | ".join(fallback_unique), len(fallback_unique)


def get_redirect_chain(response: httpx.Response) -> str:
    parts = []
    for item in response.history:
        location = item.headers.get("location", "")
        parts.append(f"{item.status_code} {item.url} -> {location}")
    parts.append(f"{response.status_code} {response.url}")
    return " | ".join(parts)


def detect_index_status(
    status_code: int | None,
    had_redirect: bool,
    meta_robots: str,
    x_robots_tag: str,
) -> str:
    robots_text = f"{meta_robots} {x_robots_tag}".lower()

    if had_redirect:
        return "redirect"

    if status_code is None:
        return "error"

    if status_code >= 400:
        return "not indexable"

    if "noindex" in robots_text:
        return "noindex"

    return "index"


async def crawl_page(
    client: httpx.AsyncClient,
    page: dict,
    settings: CrawlSettings,
) -> dict:
    url = page["URL"]
    source_sitemap = page.get("Source sitemap", "")

    empty_result = {
        "URL": url,
        "Status": "",
        "Final URL": "",
        "Redirect": "",
        "Redirect chain": "",
        "Title": "",
        "Description": "",
        "Index/noindex": "error",
        "Canonical": "",
        "H1": "",
        "H1 count": "",
        "Meta robots": "",
        "X-Robots-Tag": "",
        "Source sitemap": source_sitemap,
        "Error": "",
    }

    try:
        response = await client.get(url, follow_redirects=True)
        status = response.status_code
        final_url = str(response.url)
        had_redirect = len(response.history) > 0
        redirect_chain = get_redirect_chain(response)

        x_robots_tag = normalize_text(response.headers.get("x-robots-tag", ""))

        html = response.text or ""
        soup = BeautifulSoup(html, "html.parser")

        title = normalize_text(soup.title.get_text(" ", strip=True) if soup.title else "")
        description = get_description(soup)
        meta_robots = meta_by_name(soup, {"robots", "googlebot", "yandex"})
        canonical = get_canonical(soup)
        h1, h1_count = get_h1(soup)

        index_status = detect_index_status(
            status_code=status,
            had_redirect=had_redirect,
            meta_robots=meta_robots,
            x_robots_tag=x_robots_tag,
        )

        if settings.delay_between_requests > 0:
            await asyncio.sleep(settings.delay_between_requests)

        return {
            "URL": url,
            "Status": status,
            "Final URL": final_url,
            "Redirect": "yes" if had_redirect else "no",
            "Redirect chain": redirect_chain,
            "Title": title,
            "Description": description,
            "Index/noindex": index_status,
            "Canonical": canonical,
            "H1": h1,
            "H1 count": h1_count,
            "Meta robots": meta_robots,
            "X-Robots-Tag": x_robots_tag,
            "Source sitemap": source_sitemap,
            "Error": "",
        }

    except Exception as exc:
        empty_result["Error"] = str(exc)
        return empty_result


async def crawl_pages(
    pages: list[dict],
    settings: CrawlSettings,
    progress_bar=None,
    status_text=None,
) -> pd.DataFrame:
    headers = {"User-Agent": settings.user_agent}
    timeout = httpx.Timeout(settings.timeout)
    semaphore = asyncio.Semaphore(settings.concurrency)
    results = []
    completed = 0

    async with httpx.AsyncClient(
        headers=headers,
        timeout=timeout,
        verify=True,
        follow_redirects=True,
    ) as client:

        async def worker(page: dict):
            nonlocal completed
            async with semaphore:
                result = await crawl_page(client, page, settings)
                results.append(result)
                completed += 1

                if progress_bar:
                    progress_bar.progress(completed / len(pages))

                if status_text:
                    status_text.write(f"Обработано: {completed}/{len(pages)}")

        await asyncio.gather(*(worker(page) for page in pages))

    df = pd.DataFrame(results)

    for col in RESULT_COLUMNS:
        if col not in df.columns:
            df[col] = ""

    return df[RESULT_COLUMNS]


def make_xlsx(df: pd.DataFrame) -> bytes:
    output = io.BytesIO()

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="All", index=False)

        if "URL" in df.columns and not df.empty:
            for domain, group in df.groupby(df["URL"].map(lambda u: urlparse(str(u)).netloc or "unknown")):
                sheet_name = sheet_name_from_domain(domain)
                group.to_excel(writer, sheet_name=sheet_name, index=False)

        workbook = writer.book
        for sheet in workbook.worksheets:
            sheet.freeze_panes = "A2"

            for col in sheet.columns:
                max_length = 0
                col_letter = col[0].column_letter
                for cell in col:
                    value = "" if cell.value is None else str(cell.value)
                    max_length = max(max_length, min(len(value), 80))
                    cell.alignment = cell.alignment.copy(wrap_text=True, vertical="top")
                sheet.column_dimensions[col_letter].width = max(12, min(max_length + 2, 70))

    return output.getvalue()


def default_sitemaps() -> str:
    return "\n".join(
        [
            "https://art-lichnost.ru/page-sitemap.xml",
            "https://msk.art-lichnost.ru/page-sitemap.xml",
        ]
    )


def pages_preview_df(pages: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(pages)[["URL", "Source sitemap"]] if pages else pd.DataFrame(columns=["URL", "Source sitemap"])


def main():
    st.set_page_config(page_title=APP_TITLE, page_icon="🕷️", layout="wide")

    st.title("🕷️ SEO Sitemap Crawler")
    st.caption("Pages only: берет только прямые URL страниц из sitemap и игнорирует image:loc / ассеты.")

    with st.sidebar:
        st.header("Настройки")

        sitemap_input = st.text_area(
            "Sitemap URL — один или несколько",
            value=default_sitemaps(),
            height=110,
        )

        max_pages = st.number_input(
            "Лимит URL",
            min_value=1,
            max_value=10000,
            value=300,
            step=50,
        )

        expand_sitemap_index = st.checkbox(
            "Развернуть sitemap index",
            value=False,
            help="Включать только если вставили sitemap index и хотите собрать URL из вложенных sitemap.",
        )

        keep_assets = st.checkbox(
            "Не отсеивать ассеты",
            value=False,
            help="По умолчанию выключено: картинки, PDF, JS, CSS и другие файлы не попадают в обход.",
        )

        concurrency = st.slider("Параллельных запросов", min_value=1, max_value=30, value=5)

        timeout = st.number_input("Timeout на запрос, сек.", min_value=3, max_value=120, value=20, step=1)

        delay_between_requests = st.number_input(
            "Пауза после запроса, сек.",
            min_value=0.0,
            max_value=5.0,
            value=0.0,
            step=0.1,
        )

        user_agent = st.text_input("User-Agent", value=DEFAULT_USER_AGENT)

        preview_button = st.button("1. Показать URL из sitemap", use_container_width=True)
        crawl_button = st.button("2. Запустить обход этих URL", type="primary", use_container_width=True)

    sitemap_urls = [line.strip() for line in sitemap_input.splitlines() if line.strip()]

    settings = CrawlSettings(
        timeout=float(timeout),
        concurrency=int(concurrency),
        max_pages=int(max_pages),
        user_agent=user_agent.strip() or DEFAULT_USER_AGENT,
        delay_between_requests=float(delay_between_requests),
        keep_assets=bool(keep_assets),
    )

    if "strict_pages" not in st.session_state:
        st.session_state.strict_pages = []

    st.info(
        "Сначала нажмите `Показать URL из sitemap`. В списке не должно быть `/wp-content/uploads/`. "
        "Потом запускайте обход."
    )

    if preview_button:
        if not sitemap_urls:
            st.error("Добавьте хотя бы одну ссылку на sitemap.")
            st.stop()

        log_box = st.empty()

        with st.spinner("Собираю только прямые URL страниц из sitemap..."):
            pages = asyncio.run(
                collect_urls(
                    sitemap_urls=sitemap_urls,
                    settings=settings,
                    expand_sitemap_index=expand_sitemap_index,
                    log_box=log_box,
                )
            )

        st.session_state.strict_pages = pages

        if not pages:
            st.warning(
                "URL не найдены. Если вставили sitemap index, включите `Развернуть sitemap index` "
                "или вставьте конкретный page-sitemap.xml."
            )
        else:
            st.success(f"Найдено URL страниц: {len(pages)}")
            st.dataframe(pages_preview_df(pages), use_container_width=True, hide_index=True)

    if st.session_state.strict_pages:
        st.subheader("URL, которые пойдут в обход")
        st.caption("Краулер обойдет только этот список. image:loc и ассеты отфильтрованы.")
        st.dataframe(pages_preview_df(st.session_state.strict_pages), use_container_width=True, hide_index=True)

    if crawl_button:
        pages = st.session_state.strict_pages

        if not pages:
            st.error("Сначала нажмите `Показать URL из sitemap` и проверьте список.")
            st.stop()

        progress_bar = st.progress(0)
        status_text = st.empty()
        started = time.time()

        with st.spinner("Обхожу только показанные URL и читаю HTML..."):
            df = asyncio.run(crawl_pages(pages, settings, progress_bar=progress_bar, status_text=status_text))

        elapsed = round(time.time() - started, 1)
        st.success(f"Готово. Обработано {len(df)} URL за {elapsed} сек.")

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Всего URL", len(df))
        c2.metric("Index", int((df["Index/noindex"] == "index").sum()))
        c3.metric("Noindex", int((df["Index/noindex"] == "noindex").sum()))
        c4.metric("Redirect", int((df["Index/noindex"] == "redirect").sum()))
        c5.metric("Ошибки", int((df["Index/noindex"] == "error").sum()))

        st.dataframe(df, use_container_width=True, hide_index=True)

        xlsx_bytes = make_xlsx(df)
        st.download_button(
            "⬇️ Скачать XLSX",
            data=xlsx_bytes,
            file_name=f"seo_sitemap_crawl_{time.strftime('%Y-%m-%d_%H-%M')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )

        csv_bytes = df.to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            "⬇️ Скачать CSV",
            data=csv_bytes,
            file_name=f"seo_sitemap_crawl_{time.strftime('%Y-%m-%d_%H-%M')}.csv",
            mime="text/csv",
            use_container_width=True,
        )

    if not preview_button and not crawl_button:
        st.subheader("Что собирается")
        st.markdown(
            """
            - **URL** — прямой URL страницы из sitemap.
            - **Status** — HTTP-статус финального ответа.
            - **Final URL** — конечный адрес после редиректов.
            - **Redirect** — был ли редирект.
            - **Title** — содержимое `<title>`.
            - **Description** — `<meta name="description">`.
            - **Index/noindex** — `index`, `noindex`, `redirect`, `not indexable` или `error`.
            - **Canonical** — `<link rel="canonical">`.
            - **H1** — все видимые `<h1>` через ` | `.
            """
        )


if __name__ == "__main__":
    main()
