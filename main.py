from enum import Enum
import csv
from dataclasses import asdict
from datetime import datetime
import json
from pathlib import Path
from time import perf_counter
from typing import Optional

import typer
from loguru import logger
from m3u_parser import M3uParser
from rich import print
import polars as pl

from xmltv_parser import XMLTVParser

app = typer.Typer()


class OutputFormat(str, Enum):
    json = "json"
    csv = "csv"
    parquet = "parquet"


PROGRAM_FIELDS = [
    "channel",
    "start",
    "start_dt",
    "stop",
    "stop_dt",
    "title",
    "sub_title",
    "description",
    "date",
    "category",
    "keyword",
    "language",
    "orig_language",
    "length",
    "country",
    "episode_num",
    "is_new",
    "premiere",
    "last_chance",
]


def write_df_as(
    df: pl.DataFrame, output_format: OutputFormat, output_file: Optional[Path] = None
):
    if output_file is None:
        output_file = Path(f"output.{output_format.value}")

    if output_format == OutputFormat.json:
        df.write_json(str(output_file))
    elif output_format == OutputFormat.csv:
        df.write_csv(str(output_file))
    elif output_format == OutputFormat.parquet:
        df.write_parquet(str(output_file))
    else:
        raise ValueError(f"Unsupported output format: {output_format}")


def apply_filter_to_category(
    df: pl.DataFrame, category: str, filter: pl.Expr
) -> pl.DataFrame:
    return df.filter(~pl.col("category").eq(category) | filter)


def name_contains(text: str) -> pl.Expr:
    return pl.col("name").str.contains(text)


def replace_in_name(df: pl.DataFrame, old: str, new: str) -> pl.DataFrame:
    return df.with_columns(pl.col("name").str.replace(old, new))


def _serialize_program_row(row: dict) -> dict:
    serialized: dict = {}
    for key, value in row.items():
        if isinstance(value, datetime):
            serialized[key] = value.isoformat()
        else:
            serialized[key] = value
    return serialized


def _write_programs_csv(parser: XMLTVParser, source_file: Path, destination_file: Path) -> int:
    count = 0
    with destination_file.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=PROGRAM_FIELDS)
        writer.writeheader()
        for program in parser.iter_parse(source_file):
            writer.writerow(_serialize_program_row(asdict(program)))
            count += 1
    return count


def _write_programs_json(parser: XMLTVParser, source_file: Path, destination_file: Path) -> int:
    count = 0
    with destination_file.open("w", encoding="utf-8") as handle:
        handle.write("[")
        first = True
        for program in parser.iter_parse(source_file):
            if not first:
                handle.write(",")
            handle.write(json.dumps(_serialize_program_row(asdict(program)), ensure_ascii=False))
            first = False
            count += 1
        handle.write("]")
    return count


@app.command()
def main():
    print("Hello from iptv-parser!")


@app.command()
def parse_m3u(file: Path, output_format: OutputFormat, output_file: Path):
    started = perf_counter()
    logger.info(
        "Running parse-m3u with file={}, output_format={}, output_file={}",
        file,
        output_format.value,
        output_file,
    )

    parser = M3uParser()
    parser.parse_m3u(str(file), check_live=False)
    channels = parser.get_list()

    for channel in channels:
        channel["guide_id"] = channel.get("tvg", {}).get("id")

    df = (
        pl.DataFrame(channels)
        .drop("tvg", "country", "language")
    )

    write_df_as(df, output_format, output_file)
    elapsed = perf_counter() - started
    logger.info("parse-m3u completed in {:.3f}s", elapsed)
    print(f"Wrote {len(df)} channels to {output_file}.")


@app.command()
def parse_xmltv(file: Path, output_format: OutputFormat, output_file: Path):
    started = perf_counter()
    logger.info(
        "Running parse-xmltv with file={}, output_format={}, output_file={}",
        file,
        output_format.value,
        output_file,
    )

    parser = XMLTVParser()

    if output_format == OutputFormat.csv:
        program_count = _write_programs_csv(parser, file, output_file)
    elif output_format == OutputFormat.json:
        program_count = _write_programs_json(parser, file, output_file)
    elif output_format == OutputFormat.parquet:
        temp_csv = output_file.with_suffix(f"{output_file.suffix}.tmp.csv")
        try:
            program_count = _write_programs_csv(parser, file, temp_csv)
            pl.scan_csv(str(temp_csv), try_parse_dates=True).sink_parquet(str(output_file), engine="streaming")
        finally:
            if temp_csv.exists():
                temp_csv.unlink()
    else:
        raise ValueError(f"Unsupported output format: {output_format}")

    elapsed = perf_counter() - started
    logger.info("parse-xmltv completed in {:.3f}s", elapsed)
    print(f"Wrote {program_count} programs to {output_file}.")


if __name__ == "__main__":
    app()
