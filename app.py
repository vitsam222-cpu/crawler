import asyncio
import gzip
import io
import re
import time
from dataclasses import dataclass
from typing import Iterable
from urllib.parse import urlparse

import httpx
import pandas as pd
import streamlit as st
from bs4 import BeautifulSoup


APP_TITLE = "SEO Sitemap Crawler"


DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (compatible; SEO-Sitemap-Crawler/1.0; "
    "+https://example.com/seo-crawler)"
)


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
    follow_nested_sitemaps: bool
    delay_between_requests: float


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


def parse_sitemap_xml(xml_bytes: bytes) -> tuple[str, list[str]]:
    xml_bytes = decompress_if_needed(xml_bytes)
    soup = BeautifulSoup(xml_bytes, "xml")

    if soup.find("sitemapindex"):
        locs = [normalize_text(loc.get_text()) for loc in soup.find_all("loc")]
        return "sitemapindex", [loc for loc in locs if loc]

    if soup.find("urlset"):
        locs = [normalize_text(loc.get_text()) for loc in soup.find_all("loc")]
        return "urlset", [loc for loc in locs if loc]

    # Fallback: some generators return XML without expected root or with namespaces.
    locs = [normalize_text(loc.get_text()) for loc in soup.find_all("loc")]
    return "unknown", [loc for loc in locs if loc]


async def fetch_bytes(client: httpx.AsyncClient, url: str) -> tuple[bytes, str]:
    response = await client.get(url, follow_redirects=True)
    response.raise_for_status()
    return response.content, str(response.url)


async def collect_urls_from_sitemaps(
    sitemap_urls: list[str],
    settings: CrawlSettings,
    log_box=None,
) -> list[dict]:
    queue = list(dict.fromkeys([url.strip() for url in sitemap_urls if url.strip()]))
    visited_sitemaps = set()
    pages: list[dict] = []

    headers = {"User-Agent": settings.user_agent}

    async with httpx.AsyncClient(
        headers=headers,
        timeout=settings.timeout,
        verify=True,
        follow_redirects=True,
    ) as client:
        while queue and len(pages) < settings.max_pages:
            sitemap_url = queue.pop(0)
            if sitemap_url in visited_sitemaps:
                continue

            visited_sitemaps.add(sitemap_url)

            try:
                if log_box:
                    log_box.write(f"Загружаю sitemap: {sitemap_url}")

                content, final_sitemap_url = await fetch_bytes(client, sitemap_url)
                sitemap_type, locs = parse_sitemap_xml(content)

                if sitemap_type == "sitemapindex" and settings.follow_nested_sitemaps:
                    for loc in locs:
                        if loc not in visited_sitemaps:
                            queue.append(loc)
                    continue

                for loc in locs:
                    if len(pages) >= settings.max_pages:
                        break
                    pages.append(
                        {
                            "URL": loc,
                            "Source sitemap": sitemap_url,
                        }
                    )

            except Exception as exc:
                if log_box:
                    log_box.write(f"Ошибка sitemap: {sitemap_url} — {exc}")

    # Deduplicate URLs but keep first source sitemap.
    seen = set()
    clean_pages = []
    for item in pages:
        url = item["URL"]
        if url in seen:
            continue
        seen.add(url)
        clean_pages.append(item)

    return clean_pages[: settings.max_pages]


def meta_by_name(soup: BeautifulSoup, names: set[str]) -> str:
    values = []
    for meta in soup.find_all("meta"):
        name = (meta.get("name") or meta.get("property") or "").strip().lower()
        if name in names:
            values.append(meta.get("content") or "")
    return normalize_text(" ".join(unique_non_empty(values)))


def get_description(soup: BeautifulSoup) -> str:
    # Primary: standard meta description.
    value = meta_by_name(soup, {"description"})
    if value:
        return value

    # Fallbacks are not true meta description, but useful for diagnostics.
    return ""


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

        classes = " ".join(current.get("class", [])) if isinstance(current.get("class"), list) else str(current.get("class", ""))
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

        content_type = response.headers.get("content-type", "")
        x_robots_tag = normalize_text(response.headers.get("x-robots-tag", ""))

        html = response.text if "html" in content_type.lower() or response.text else ""
        soup = BeautifulSoup(html, "lxml")

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

        if "URL" in df.columns:
            for domain, group in df.groupby(df["URL"].map(lambda u: urlparse(str(u)).netloc or "unknown")):
                sheet_name = sheet_name_from_domain(domain)
                group.to_excel(writer, sheet_name=sheet_name, index=False)

        workbook = writer.book
        for sheet in workbook.worksheets:
            sheet.freeze_panes = "A2"
            widths = {
                "A": 46,
                "B": 10,
                "C": 46,
                "D": 12,
                "E": 70,
                "F": 44,
                "G": 70,
                "H": 16,
                "I": 44,
                "J": 60,
                "K": 10,
                "L": 28,
                "M": 28,
                "N": 46,
                "O": 40,
            }

            for letter, width in widths.items():
                sheet.column_dimensions[letter].width = width

            for row in sheet.iter_rows():
                for cell in row:
                    cell.alignment = cell.alignment.copy(wrap_text=True, vertical="top")

    return output.getvalue()


def load_example_urls() -> str:
    return "\n".join(
        [
            "https://art-lichnost.ru/page-sitemap.xml",
            "https://msk.art-lichnost.ru/page-sitemap.xml",
        ]
    )


def main():
    st.set_page_config(
        page_title=APP_TITLE,
        page_icon="🕷️",
        layout="wide",
    )

    st.title("🕷️ SEO Sitemap Crawler")
    st.caption(
        "Серверный краулер: берет URL из sitemap, заходит на каждую страницу, "
        "читает HTML и выгружает Title, Description, Index/noindex, Canonical, H1."
    )

    with st.sidebar:
        st.header("Настройки")

        sitemap_input = st.text_area(
            "Sitemap URL — один или несколько",
            value=load_example_urls(),
            height=110,
            help="Каждая ссылка с новой строки. Можно вставлять sitemap.xml, page-sitemap.xml или sitemap index.",
        )

        max_pages = st.number_input(
            "Лимит URL",
            min_value=1,
            max_value=10000,
            value=300,
            step=50,
        )

        concurrency = st.slider(
            "Параллельных запросов",
            min_value=1,
            max_value=30,
            value=5,
            help="Для чужих сайтов лучше 3–5. Для своего сайта можно выше.",
        )

        timeout = st.number_input(
            "Timeout на запрос, сек.",
            min_value=3,
            max_value=120,
            value=20,
            step=1,
        )

        delay_between_requests = st.number_input(
            "Пауза после запроса, сек.",
            min_value=0.0,
            max_value=5.0,
            value=0.0,
            step=0.1,
        )

        follow_nested_sitemaps = st.checkbox(
            "Переходить во вложенные sitemap из sitemap index",
            value=True,
        )

        user_agent = st.text_input(
            "User-Agent",
            value=DEFAULT_USER_AGENT,
        )

        run_button = st.button("🚀 Запустить краулер", type="primary", use_container_width=True)

    st.info(
        "Результат можно скачать в XLSX. В Excel будет общий лист `All` и отдельные листы по доменам."
    )

    if run_button:
        sitemap_urls = [line.strip() for line in sitemap_input.splitlines() if line.strip()]

        if not sitemap_urls:
            st.error("Добавьте хотя бы одну ссылку на sitemap.")
            st.stop()

        settings = CrawlSettings(
            timeout=float(timeout),
            concurrency=int(concurrency),
            max_pages=int(max_pages),
            user_agent=user_agent.strip() or DEFAULT_USER_AGENT,
            follow_nested_sitemaps=bool(follow_nested_sitemaps),
            delay_between_requests=float(delay_between_requests),
        )

        log_box = st.empty()
        progress_bar = st.progress(0)
        status_text = st.empty()

        started = time.time()

        with st.spinner("Собираю URL из sitemap..."):
            pages = asyncio.run(collect_urls_from_sitemaps(sitemap_urls, settings, log_box=log_box))

        if not pages:
            st.error("Не удалось найти URL в sitemap. Проверьте ссылку и доступность файла.")
            st.stop()

        st.success(f"Найдено URL: {len(pages)}")

        with st.spinner("Обхожу страницы и читаю HTML..."):
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

    else:
        st.subheader("Что собирается")
        st.markdown(
            """
            - **URL** — адрес из sitemap.
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
