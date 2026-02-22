from __future__ import annotations

from datetime import datetime
from pathlib import Path
from time import perf_counter

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger
import polars as pl
from pydantic import BaseModel
import pytz


ROOT = Path(__file__).resolve().parent
CHANNELS_PARQUET = ROOT / "channels.parquet"
EPG_PARQUET = ROOT / "epg.parquet"


class CategoryResponse(BaseModel):
    name: str


class ChannelResponse(BaseModel):
    name: str | None
    logo: str | None
    url: str | None
    category: str | None
    guide_id: str | None


class ProgramResponse(BaseModel):
    channel: str | None
    start_dt: datetime | None
    stop_dt: datetime | None
    title: str | None
    description: str | None


app = FastAPI(title="IPTV Parser API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _ensure_inputs_exist() -> None:
    missing: list[str] = []
    if not CHANNELS_PARQUET.exists():
        missing.append(str(CHANNELS_PARQUET.name))
    if not EPG_PARQUET.exists():
        missing.append(str(EPG_PARQUET.name))

    if missing:
        raise HTTPException(
            status_code=500,
            detail=f"Missing required parquet file(s): {', '.join(missing)}",
        )


@app.get("/categories", response_model=list[CategoryResponse])
def get_categories() -> list[CategoryResponse]:
    _ensure_inputs_exist()
    started = perf_counter()

    categories_df = (
        pl.scan_parquet(CHANNELS_PARQUET)
        .select(pl.col("category").alias("name"))
        .drop_nulls()
        .unique()
        .sort("name")
        .collect()
    )

    rows = categories_df.to_dicts()
    result = [CategoryResponse(**row) for row in rows]
    logger.info("GET /categories -> {} rows in {:.3f}s", len(result), perf_counter() - started)
    return result


@app.get("/categories/{category}/channels", response_model=list[ChannelResponse])
def get_channels_by_category(category: str) -> list[ChannelResponse]:
    _ensure_inputs_exist()
    started = perf_counter()

    channels_df = (
        pl.scan_parquet(CHANNELS_PARQUET)
        .filter(pl.col("category") == category)
        .select(["name", "logo", "url", "category", "guide_id"])
        .sort("name")
        .collect()
    )

    rows = channels_df.to_dicts()
    result = [ChannelResponse(**row) for row in rows]
    logger.info(
        "GET /categories/{}/channels -> {} rows in {:.3f}s",
        category,
        len(result),
        perf_counter() - started,
    )
    return result


@app.get("/channels/{channel}/programs", response_model=list[ProgramResponse])
def get_programs_by_channel(channel: str) -> list[ProgramResponse]:
    _ensure_inputs_exist()
    started = perf_counter()

    programs_df = (
        pl.scan_parquet(EPG_PARQUET)
        .filter(
            pl.col("channel").eq(channel),
            pl.col("stop_dt").gt(pl.lit(datetime.now(pytz.UTC))),
        )
        .with_columns(
            pl.col("start_dt", "stop_dt").dt.convert_time_zone("America/New_York")
        )
        .select(
            [
                "channel",
                "start_dt",
                "stop_dt",
                "title",
                "description",
            ]
        )
        .unique(["channel", "start_dt", "title"])
        .sort("start_dt")
        .collect()
    )

    rows = programs_df.to_dicts()
    result = [ProgramResponse(**row) for row in rows]
    logger.info(
        "GET /channels/{}/programs -> {} rows in {:.3f}s",
        channel,
        len(result),
        perf_counter() - started,
    )
    return result
