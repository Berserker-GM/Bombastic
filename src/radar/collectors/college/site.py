"""Generic college notice list. The caller supplies the CSS selectors."""

import hashlib
from datetime import date
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup, Tag
from pydantic import BaseModel, ConfigDict, field_validator
from sqlalchemy import select
from sqlalchemy.orm import Session

from radar.collectors.base import Collector, CollectResult, RawEvent
from radar.llm.client import LLM
from radar.llm.extract import (
    ExtractionError,
    extract_notice,
    normalize_text,
    to_raw_event,
)
from radar.models import Event
from radar.models.enums import EventCategory

_TIMEOUT_SECONDS = 30.0


class NoticeListError(Exception):
    """The notices page did not yield a usable list."""


class NoticeSelectors(BaseModel):
    model_config = ConfigDict(extra="ignore")

    title: str
    link: str
    date: str

    @field_validator("title", "link", "date")
    @classmethod
    def _not_blank(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("selector is empty")
        return stripped


class ListedNotice(BaseModel):
    model_config = ConfigDict(extra="ignore")

    title: str
    url: str
    listed_date: str | None = None


class CollegeSiteCollector(Collector):
    def __init__(
        self,
        *,
        list_url: str,
        selectors: NoticeSelectors,
        llm: LLM,
        seen: set[tuple[str, str]] | None = None,
        source_name: str = "college",
        today: date | None = None,
    ) -> None:
        self.list_url = list_url
        self.selectors = selectors
        self.llm = llm
        self.seen = set(seen or ())
        self.source_name = source_name
        self.today = today

    async def collect(self) -> CollectResult:
        try:
            return await super().collect()
        except NoticeListError as exc:
            return CollectResult(status="malformed", events=[], error=str(exc))
        except ExtractionError as exc:
            return CollectResult(status="malformed", events=[], error=str(exc))

    async def fetch(self) -> list[RawEvent]:
        async with httpx.AsyncClient(
            timeout=_TIMEOUT_SECONDS,
            follow_redirects=True,
        ) as client:
            list_response = await client.get(self.list_url)
            list_response.raise_for_status()
            listed = parse_notice_list(
                list_response.text, self.selectors, self.list_url
            )
            if not listed:
                raise NoticeListError("notice selectors matched nothing")
            events: list[RawEvent] = []
            for item in listed:
                detail = await client.get(item.url)
                detail.raise_for_status()
                text = html_to_text(detail.text)
                digest = notice_content_hash(text)
                if (item.url, digest) in self.seen:
                    continue
                notice = extract_notice(text, self.llm, today=self.today)
                raw = to_raw_event(
                    notice,
                    source_name=self.source_name,
                    url=item.url,
                    notice_hash=digest,
                    list_title=item.title,
                )
                if raw is None:
                    continue
                extra = dict(raw.extra or {})
                extra["notice_category"] = notice.category.value
                events.append(
                    raw.model_copy(
                        update={
                            "category": EventCategory.COLLEGE,
                            "extra": extra,
                        }
                    )
                )
            return events


def parse_notice_list(
    html: str, selectors: NoticeSelectors, base_url: str
) -> list[ListedNotice]:
    soup = BeautifulSoup(html, "html.parser")
    titles = soup.select(selectors.title)
    links = soup.select(selectors.link)
    dates = soup.select(selectors.date)
    if not titles or not links:
        return []
    if len(titles) != len(links):
        raise NoticeListError(
            "notice title and link selectors matched different counts"
        )
    notices: list[ListedNotice] = []
    seen_urls: set[str] = set()
    for index, (title_node, link_node) in enumerate(zip(titles, links, strict=True)):
        url = _absolute_http_url(base_url, _href(link_node))
        if url is None or url in seen_urls:
            continue
        seen_urls.add(url)
        listed_date = None
        if index < len(dates):
            listed_date = _node_text(dates[index]) or None
        notices.append(
            ListedNotice(title=_node_text(title_node), url=url, listed_date=listed_date)
        )
    return notices


def html_to_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style"]):
        tag.decompose()
    return soup.get_text("\n", strip=True)


def notice_content_hash(text: str) -> str:
    return hashlib.sha256(normalize_text(text).encode("utf-8")).hexdigest()


def load_seen_notices(session: Session, source_id: int) -> set[tuple[str, str]]:
    rows = session.scalars(select(Event).where(Event.source_id == source_id)).all()
    seen: set[tuple[str, str]] = set()
    for event in rows:
        if not event.external_id or not isinstance(event.extra, dict):
            continue
        notice_hash = event.extra.get("notice_hash")
        if isinstance(notice_hash, str):
            seen.add((event.external_id, notice_hash))
    return seen


def _href(node: Tag) -> str | None:
    raw = node.get("href")
    if raw is None and node.name != "a":
        anchor = node.select_one("a[href]")
        raw = None if anchor is None else anchor.get("href")
    if isinstance(raw, list):
        raw = raw[0] if raw else None
    if not isinstance(raw, str):
        return None
    stripped = raw.strip()
    return stripped or None


def _absolute_http_url(base_url: str, href: str | None) -> str | None:
    if href is None:
        return None
    absolute = urljoin(base_url, href)
    parsed = urlparse(absolute)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        return None
    return absolute


def _node_text(node: Tag) -> str:
    return " ".join(node.get_text(" ", strip=True).split())
