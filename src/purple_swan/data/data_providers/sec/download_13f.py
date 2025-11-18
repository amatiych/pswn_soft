from __future__ import annotations

import logging
import os
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
from sec_api import QueryApi  # type: ignore[import-not-found]

logger = logging.getLogger(__name__)

DEFAULT_BATCH_SIZE = 50
SUMMARY_FIELDS = (
    "cik",
    "companyName",
    "companyNameLong",
    "ticker",
    "linkToTxt",
    "linkToHtml",
    "linkToFilingDetails",
)


def get_sec_query_api(api_key: Optional[str] = None) -> QueryApi:
    """
    Instantiate a QueryApi client using the SEC.io API key from the environment unless provided.
    """
    api_key = api_key or os.getenv("SEC_IO_API_KEY")
    if not api_key:
        raise RuntimeError("SEC_IO_API_KEY environment variable is not set.")
    return QueryApi(api_key=api_key)


def build_13f_query(start_date: str, end_date: str) -> str:
    """
    Build a 13F search query covering the inclusive date range.
    """
    return f'formType:"13F" AND filedAt:[{start_date} TO {end_date}]'


def iter_filings(
    query_api: QueryApi,
    search_query: str,
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    start_offset: int = 0,
    max_batches: Optional[int] = None,
) -> Iterable[Tuple[int, List[Dict[str, Any]]]]:
    """
    Yield batches of filings returned by the SEC API.
    """
    batch_index = 0
    while max_batches is None or batch_index < max_batches:
        offset = start_offset + batch_index * batch_size
        params = {
            "query": search_query,
            "from": str(offset),
            "size": str(batch_size),
            "sort": [{"filedAt": {"order": "desc"}}],
        }

        response = query_api.get_filings(params)
        filings = response.get("filings", [])
        if not filings:
            logger.info("No filings returned for offset %s; stopping.", offset)
            break

        yield batch_index, filings
        batch_index += 1

        if len(filings) < batch_size:
            logger.info(
                "Batch %s returned %s filings (less than batch size); stopping.",
                batch_index,
                len(filings),
            )
            break


def _write_dataframe_pair(df: pd.DataFrame, csv_uri: str, *, index: bool, header: bool = True) -> None:
    """
    Write a DataFrame to both CSV and Parquet representations sharing the same base URI.
    """
    df.to_csv(csv_uri, index=index, header=header)

    if csv_uri.endswith(".csv"):
        parquet_uri = f"{csv_uri[:-4]}.parquet"
    else:
        parquet_uri = f"{csv_uri}.parquet"

    df.to_parquet(parquet_uri, compression="snappy", index=index)


def download_13f_filings(
    search_query: str,
    *,
    quarter: str,
    storage_prefix: str,
    api_key: Optional[str] = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    max_batches: Optional[int] = None,
    start_offset: int = 0,
    holdings_from_batch: int = 0,
) -> List[Dict[str, Any]]:
    """
    Download 13F filings matching the query and persist summary and holdings data frames to storage.

    Args:
        search_query: SEC API search string.
        quarter: Quarter string used in output paths, e.g. "2025Q3".
        storage_prefix: Base URI where results should be written (e.g. S3 prefix).
        api_key: Optional SEC.io API key; falls back to the SEC_IO_API_KEY environment variable.
        batch_size: Number of filings fetched per request.
        max_batches: Optional maximum number of batches to retrieve.
        start_offset: Offset for the first record (0-based).
        holdings_from_batch: First batch index (0-based) from which holdings should be persisted.

    Returns:
        A list of summary records for all processed filings.
    """
    query_api = get_sec_query_api(api_key)
    summary_records: List[Dict[str, Any]] = []

    for batch_index, filings in iter_filings(
        query_api,
        search_query,
        batch_size=batch_size,
        start_offset=start_offset,
        max_batches=max_batches,
    ):
        logger.info("Processing batch %s with %s filings.", batch_index, len(filings))
        batch_summary: List[Dict[str, Any]] = []

        for filing in filings:
            batch_summary.append({field: filing.get(field) for field in SUMMARY_FIELDS})

            if batch_index >= holdings_from_batch:
                holdings = filing.get("holdings") or []
                if holdings:
                    holdings_df = pd.DataFrame(holdings)
                    if not holdings_df.empty and "ticker" in holdings_df.columns:
                        holdings_df = holdings_df.set_index("ticker")

                    cik = filing.get("cik")
                    if cik:
                        holdings_uri = f"{storage_prefix}/quarter={quarter}/cik={cik}/holdings.csv"
                        _write_dataframe_pair(holdings_df, holdings_uri, index=True, header=True)
                    else:
                        logger.warning("Skipping holdings write due to missing CIK: %s", filing)

        if batch_summary:
            summary_records.extend(batch_summary)
            batch_uri = f"{storage_prefix}/quarter={quarter}/summary_{batch_index}.csv"
            _write_dataframe_pair(pd.DataFrame(batch_summary), batch_uri, index=False, header=True)

    if summary_records:
        summary_uri = f"{storage_prefix}/quarter={quarter}/company_list.csv"
        _write_dataframe_pair(pd.DataFrame(summary_records), summary_uri, index=False, header=True)

    return summary_records


__all__ = [
    "build_13f_query",
    "download_13f_filings",
    "get_sec_query_api",
]