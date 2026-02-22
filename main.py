from enum import Enum
from dataclasses import asdict
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


KEEP_CATEGORIES = [
    "USA SPORTS",
    "INTERNATIONAL SPORTS",
    "NEWS",
    "USA LOCALS",
    "4K (UHD)",
    "USA NETWORKS",
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
        .filter(pl.col("category").is_in(KEEP_CATEGORIES))
        .pipe(apply_filter_to_category, "USA LOCALS", name_contains("Philadelphia"))
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
    programs = parser.parse(file)
    rows = [asdict(program) for program in programs]

    df = pl.DataFrame(rows)
    write_df_as(df, output_format, output_file)
    elapsed = perf_counter() - started
    logger.info("parse-xmltv completed in {:.3f}s", elapsed)
    print(f"Wrote {len(df)} programs to {output_file}.")


if __name__ == "__main__":
    app()
