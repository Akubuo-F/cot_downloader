from typing import Any
from sodapy import Socrata
from cot_downloader.constants import DOMAIN, DESC_ORDER, LEGACY_FUTUTRES_ONLY 

class COTDownloader:
    """Downloads the Commitment of Traders (COT) reports from the CFTC database."""

    @staticmethod
    def download(
        app_token: str, 
        market_and_exchange_names: list[str], 
        limit: int = 1000, 
        order: str = DESC_ORDER
    ) -> list[dict[str, Any]] | list[list[str]]:
        """Connects to the Socrata API using the provided app token and retrieves COT
        data filtered by market and exchange names.

        Params:
            app_token (str): The Socrata API application token for authentication.
                Required for API access. Get one from https://dev.socrata.com/
            market_and_exchange_names (list[str]): list of market and exchange names
                to filter the results. Use exact names as they appear in the CFTC database.
                Examples: ["EURO FX - CHICAGO MERCANTILE EXCHANGE", "CRUDE OIL, LIGHT SWEET - NEW YORK MERCANTILE EXCHANGE"]
            limit (int, optional): Maximum number of records to retrieve. 
                Defaults to 1000. Maximum allowed by API is typically 50,000.
            order (str, optional): Sort order for results. Defaults to DESC_ORDER.
                Use constants from cot_downloader.constants module.
        
        Returns:
            COT report data as a list
            of dictionaries where each dictionary represents a single report record,
            or as a list of lists for raw data format.
        
        Raises:
            ConnectionError: When the API request fails due to network issues,
                authentication problems, invalid parameters, or API errors.
                The original exception is chained for debugging purposes.
        
        Example:
            >>> reports = COTDownloader.download(
            ...     app_token="YOUR-API-TOKEN",
            ...     market_and_exchange_names=["EURO FX - CHICAGO MERCANTILE EXCHANGE"],
            ...     limit=5
            ... )
            >>> for report in reports:
            ...     print(f"Date: {report.get('report_date_as_yyyy_mm_dd')}")
            ...     print(f"Market: {report.get('market_and_exchange_names')}")
        """
        try:
            with Socrata(DOMAIN, app_token) as client:
                where_clause = COTDownloader._build_where_clause(market_and_exchange_names)
                return client.get(
                    LEGACY_FUTUTRES_ONLY, 
                    where=where_clause,
                    limit = limit,
                    order = order
                )
        except Exception as e:
            raise ConnectionError(f"Download failed due to {e}") from e

    @staticmethod
    def _build_where_clause(market_and_exchange_names: list[str]) -> str:
        if not market_and_exchange_names:
            return "1-1"
        
        conditions = []
        for name in market_and_exchange_names:
            cleaned_name = name.replace("'", "''")
            condition = f"market_and_exchange_names = '{cleaned_name}'"
            conditions.append(condition)
        
        return " OR ".join(conditions)
