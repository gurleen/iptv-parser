from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import re
from time import perf_counter
from typing import Iterator
import xml.etree.ElementTree as ET

from loguru import logger
from tqdm import tqdm


_XMLTV_DATETIME_RE = re.compile(
    r"^(?P<stamp>\d{8}(?:\d{2}(?:\d{2}(?:\d{2})?)?)?)(?:\s+(?P<tz>[+-]\d{4}))?$"
)


@dataclass(slots=True)
class XMLTVProgram:
    channel: str | None
    start: str | None
    start_dt: datetime | None
    stop: str | None
    stop_dt: datetime | None
    title: str | None
    sub_title: str | None
    description: str | None
    date: str | None
    category: str | None
    keyword: str | None
    language: str | None
    orig_language: str | None
    length: str | None
    country: str | None
    episode_num: str | None
    is_new: bool
    premiere: str | None
    last_chance: str | None


class XMLTVParser:
    """Parse XMLTV-like files into Python dictionaries.

    The parser is tolerant of optional fields and repeated elements and
    preserves both raw values and parsed datetime values where relevant.
    """

    def parse(self, file_path: str | Path) -> list[XMLTVProgram]:
        return list(self.iter_parse(file_path))

    def iter_parse(self, file_path: str | Path) -> Iterator[XMLTVProgram]:
        parse_start = perf_counter()
        source = Path(file_path)
        logger.info("Starting XMLTV parse: {}", source)

        context = ET.iterparse(str(file_path), events=("start", "end"))
        root: ET.Element | None = None
        processed = 0

        with tqdm(desc="Parsing XMLTV programmes", unit="programme") as progress:
            for event, element in context:
                if root is None and event == "start":
                    if element.tag != "tv":
                        raise ValueError("Expected root element <tv> in XMLTV file.")
                    root = element

                if event == "end" and element.tag == "programme":
                    processed += 1
                    progress.update(1)
                    yield self._parse_programme(element)
                    element.clear()
                    if root is not None:
                        root.clear()

        elapsed = perf_counter() - parse_start
        logger.info(
            "Finished XMLTV parse: {} programmes from {} in {:.3f}s",
            processed,
            source,
            elapsed,
        )

    def _parse_programme(self, element: ET.Element) -> XMLTVProgram:
        start_raw = element.get("start")
        stop_raw = element.get("stop")

        return XMLTVProgram(
            channel=element.get("channel"),
            start=start_raw,
            start_dt=self.parse_xmltv_datetime(start_raw),
            stop=stop_raw,
            stop_dt=self.parse_xmltv_datetime(stop_raw),
            title=self._text_or_none(element.find("title")),
            sub_title=self._text_or_none(element.find("sub-title")),
            description=self._text_or_none(element.find("desc")),
            date=self._text_or_none(element.find("date")),
            category=self._text_or_none(element.find("category")),
            keyword=self._text_or_none(element.find("keyword")),
            language=self._text_or_none(element.find("language")),
            orig_language=self._text_or_none(element.find("orig-language")),
            length=self._text_or_none(element.find("length")),
            country=self._text_or_none(element.find("country")),
            episode_num=self._text_or_none(element.find("episode-num")),
            is_new=element.find("new") is not None,
            premiere=self._text_or_none(element.find("premiere")),
            last_chance=self._text_or_none(element.find("last-chance")),
        )

    @staticmethod
    def parse_xmltv_datetime(value: str | None) -> datetime | None:
        if not value:
            return None

        match = _XMLTV_DATETIME_RE.match(value.strip())
        if match is None:
            return None

        stamp = match.group("stamp")
        tz = match.group("tz")

        if len(stamp) == 8:
            fmt = "%Y%m%d"
        elif len(stamp) == 10:
            fmt = "%Y%m%d%H"
        elif len(stamp) == 12:
            fmt = "%Y%m%d%H%M"
        elif len(stamp) == 14:
            fmt = "%Y%m%d%H%M%S"
        else:
            return None

        try:
            if tz:
                return datetime.strptime(f"{stamp} {tz}", f"{fmt} %z")
            return datetime.strptime(stamp, fmt)
        except ValueError:
            return None

    @staticmethod
    def _text_or_none(element: ET.Element | None) -> str | None:
        if element is None or element.text is None:
            return None
        text = element.text.strip()
        return text or None

    # Additional XMLTV fields are intentionally omitted from XMLTVProgram to keep
    # the returned type flat and single-valued.
