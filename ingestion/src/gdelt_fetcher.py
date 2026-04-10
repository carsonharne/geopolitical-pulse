"""
GDELT Fetcher Module for The Geopolitical Supply Chain Pulse

This module fetches, processes, and filters GDELT 2.0 event data
for supply chain and economic indicators related to the Chinese market.

Technical Stack:
    - Python 3.x
    - httpx: Async HTTP client for fetching data
    - polars: High-performance DataFrame library for data processing
"""

from __future__ import annotations

import io
import json
import re
import zipfile
from typing import Any

import httpx
import polars as pl


class GdeltFetcher:
    """
    A robust fetcher for GDELT 2.0 event data.

    This class performs the following pipeline:
    1. Fetches the GDELT 2.0 Last Update text file
    2. Extracts the URL for the latest 15-minute English Translation export CSV
    3. Downloads the ZIP file in memory, extracts the CSV, and reads into Polars DataFrame
    4. Filters for Chinese market events with specific CAMEO event codes
    5. Returns clean JSON-ready data

    Attributes:
        last_update_url (str): URL for the GDELT lastupdate.txt file
        timeout (int): HTTP request timeout in seconds
    """

    # GDELT 2.0 column names for the events export file
    # Source: http://data.gdeltproject.org/documentation/GDELT-Data_Format_Codebook.pdf
    GDELT_COLUMNS = [
        "GLOBALEVENTID",
        "SQLDATE",
        "MonthYear",
        "Year",
        "FractionDate",
        "Actor1Code",
        "Actor1Name",
        "Actor1CountryCode",
        "Actor1KnownGroupCode",
        "Actor1EthnicCode",
        "Actor1Religion1Code",
        "Actor1Religion2Code",
        "Actor1Type1Code",
        "Actor1Type2Code",
        "Actor1Type3Code",
        "Actor2Code",
        "Actor2Name",
        "Actor2CountryCode",
        "Actor2KnownGroupCode",
        "Actor2EthnicCode",
        "Actor2Religion1Code",
        "Actor2Religion2Code",
        "Actor2Type1Code",
        "Actor2Type2Code",
        "Actor2Type3Code",
        "IsRootEvent",
        "EventCode",
        "EventBaseCode",
        "EventRootCode",
        "QuadClass",
        "GoldsteinScale",
        "NumMentions",
        "NumSources",
        "NumArticles",
        "AvgTone",
        "Actor1Geo_Type",
        "Actor1Geo_Fullname",
        "Actor1Geo_CountryCode",
        "Actor1Geo_ADM1Code",
        "Actor1Geo_ADM2Code",
        "Actor1Geo_Lat",
        "Actor1Geo_Long",
        "Actor1Geo_FeatureID",
        "Actor2Geo_Type",
        "Actor2Geo_Fullname",
        "Actor2Geo_CountryCode",
        "Actor2Geo_ADM1Code",
        "Actor2Geo_ADM2Code",
        "Actor2Geo_Lat",
        "Actor2Geo_Long",
        "Actor2Geo_FeatureID",
        "ActionGeo_Type",
        "ActionGeo_Fullname",
        "ActionGeo_CountryCode",
        "ActionGeo_ADM1Code",
        "ActionGeo_ADM2Code",
        "ActionGeo_Lat",
        "ActionGeo_Long",
        "ActionGeo_FeatureID",
        "DATEADDED",
        "SOURCEURL",
    ]

    # Target CAMEO event codes for supply chain/economic indicators
    TARGET_EVENT_CODES = ["03", "04", "10"]

    # Columns to keep for downstream warehouse
    OUTPUT_COLUMNS = [
        "GLOBALEVENTID",
        "SQLDATE",
        "EventCode",
        "GoldsteinScale",
        "NumMentions",
        "ActionGeo_CountryCode",
        "SOURCEURL",
    ]

    def __init__(self, timeout: int = 60) -> None:
        """
        Initialize the GdeltFetcher.

        Args:
            timeout: HTTP request timeout in seconds (default: 60)
        """
        self.base_url = "http://data.gdeltproject.org/gdeltv2/"
        self.last_update_url = self.base_url + "lastupdate.txt"
        self.timeout = timeout

    def fetch_last_update(self, client: httpx.Client) -> str | None:
        """
        Fetch the GDELT lastupdate.txt file and extract the export CSV URL.

        Args:
            client: httpx Client instance for making HTTP requests

        Returns:
            The URL for the latest export.CSV.zip file, or None if not found/error
        """
        try:
            response = client.get(self.last_update_url, timeout=self.timeout)
            response.raise_for_status()
            content = response.text

            # Parse to find the export.CSV.zip URL for English Translation
            # The file contains lines like:
            # <size> <hash> <filename>
            # We want the line ending in export.CSV.zip
            for line in content.strip().split("\n"):
                parts = line.strip().split()
                if len(parts) >= 3:
                    url_part = parts[2]
                    if url_part.endswith("export.CSV.zip"):
                        # Construct full URL - extract just the filename from the URL
                        # In case the file contains a full URL, extract the filename
                        if url_part.startswith("http://") or url_part.startswith("https://"):
                            # Extract just the filename from the URL
                            filename = url_part.split("/")[-1]
                        else:
                            filename = url_part
                        # Construct proper full URL
                        return self.base_url + filename

            print("Warning: Could not find export.CSV.zip URL in lastupdate.txt")
            return None

        except httpx.HTTPStatusError as e:
            print(f"HTTP error fetching lastupdate.txt: {e.response.status_code}")
            return None
        except httpx.RequestError as e:
            print(f"Network error fetching lastupdate.txt: {e}")
            return None
        except Exception as e:
            print(f"Unexpected error fetching lastupdate.txt: {e}")
            return None

    def download_and_extract_csv(
        self, client: httpx.Client, csv_url: str
    ) -> pl.DataFrame | None:
        """
        Download the ZIP file, extract CSV, and load into Polars DataFrame.

        Args:
            client: httpx Client instance for making HTTP requests
            csv_url: URL of the ZIP file containing the CSV

        Returns:
            Polars DataFrame with GDELT data, or None on error
        """
        try:
            print(f"Downloading: {csv_url}")
            response = client.get(csv_url, timeout=self.timeout)
            response.raise_for_status()

            # Read ZIP file in memory
            zip_bytes = io.BytesIO(response.content)

            with zipfile.ZipFile(zip_bytes, "r") as zf:
                # Find the CSV file inside the ZIP (case-insensitive)
                csv_files = [name for name in zf.namelist() if name.lower().endswith(".csv")]
                if not csv_files:
                    print("Error: No CSV file found in ZIP archive")
                    return None

                csv_name = csv_files[0]
                print(f"Extracting: {csv_name}")

                # Extract and read CSV content
                with zf.open(csv_name) as csv_file:
                    csv_content = csv_file.read()

            # Read into Polars DataFrame
            # GDELT TSVs use tab separator and have no header row
            df = pl.read_csv(
                io.BytesIO(csv_content),
                separator="\t",
                has_header=False,
                new_columns=self.GDELT_COLUMNS,
                encoding="utf8",
            )

            print(f"Loaded {df.height} rows from GDELT export")
            return df

        except httpx.HTTPStatusError as e:
            print(f"HTTP error downloading ZIP: {e.response.status_code}")
            return None
        except httpx.RequestError as e:
            print(f"Network error downloading ZIP: {e}")
            return None
        except zipfile.BadZipFile as e:
            print(f"Error: Invalid ZIP file: {e}")
            return None
        except Exception as e:
            print(f"Unexpected error processing ZIP: {e}")
            return None

    def filter_china_events(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Filter DataFrame for Chinese market events with target CAMEO codes.

        Args:
            df: Raw GDELT DataFrame

        Returns:
            Filtered DataFrame with only relevant records
        """
        # Cast EventCode to string for proper string matching
        # (Polars may infer it as integer from the CSV data)
        df = df.with_columns(pl.col("EventCode").cast(pl.Utf8))

        # Filter for China (ActionGeo_CountryCode == 'CH')
        # and target event codes (03, 04, 10)
        # We check EventCode starts with any of the target codes
        filtered = df.filter(
            (pl.col("ActionGeo_CountryCode") == "CH")
            & (
                pl.col("EventCode").str.starts_with("03")
                | pl.col("EventCode").str.starts_with("04")
                | pl.col("EventCode").str.starts_with("10")
            )
        )

        print(f"Filtered to {filtered.height} China-related events")
        return filtered

    def select_output_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Select only the essential columns for downstream warehouse.

        Args:
            df: Filtered DataFrame

        Returns:
            DataFrame with only OUTPUT_COLUMNS
        """
        # Only select columns that exist in the dataframe
        available_cols = [col for col in self.OUTPUT_COLUMNS if col in df.columns]
        return df.select(available_cols)

    def to_json_records(self, df: pl.DataFrame) -> list[dict[str, Any]]:
        """
        Convert DataFrame to list of dictionaries (JSON-ready records).

        Args:
            df: Final DataFrame

        Returns:
            List of dictionaries ready for Kafka or JSON serialization
        """
        return df.to_dicts()

    def to_json_string(self, df: pl.DataFrame, indent: int | None = None) -> str:
        """
        Convert DataFrame to JSON string.

        Args:
            df: Final DataFrame
            indent: Optional indentation for pretty printing

        Returns:
            JSON string representation of the data
        """
        records = self.to_json_records(df)
        return json.dumps(records, indent=indent, default=str)

    def run(self) -> list[dict[str, Any]] | None:
        """
        Execute the full GDELT fetch pipeline.

        Returns:
            List of filtered event dictionaries ready for downstream processing,
            or None if any step fails
        """
        try:
            with httpx.Client() as client:
                # Step 1: Fetch last update URL
                csv_url = self.fetch_last_update(client)
                if csv_url is None:
                    return None

                print(f"Found export URL: {csv_url}")

                # Step 2: Download and extract CSV
                df = self.download_and_extract_csv(client, csv_url)
                if df is None:
                    return None

                # Step 3: Filter for China events
                filtered_df = self.filter_china_events(df)

                # Step 4: Select output columns
                output_df = self.select_output_columns(filtered_df)

                # Step 5: Convert to records
                records = self.to_json_records(output_df)

                return records

        except Exception as e:
            print(f"Pipeline execution error: {e}")
            return None


def main() -> None:
    """
    Main entry point for testing the GdeltFetcher.

    Fetches the latest GDELT data, filters for Chinese supply chain events,
    and prints the results to console.
    """
    print("=" * 60)
    print("The Geopolitical Supply Chain Pulse - GDELT Fetcher")
    print("=" * 60)
    print()

    fetcher = GdeltFetcher()
    records = fetcher.run()

    if records is None:
        print("\nFailed to fetch GDELT data")
        return

    print()
    print("=" * 60)
    print(f"Results: {len(records)} filtered records")
    print("=" * 60)
    print()

    if records:
        # Print first 5 records as formatted JSON
        print("Sample records (first 5):")
        print("-" * 60)
        for i, record in enumerate(records[:5], 1):
            print(f"\nRecord {i}:")
            print(json.dumps(record, indent=2, default=str))

        print()
        print("-" * 60)
        print(f"Total records ready for Kafka: {len(records)}")
    else:
        print("No matching records found for the current time period.")


if __name__ == "__main__":
    main()
