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
    ) -> dict[str, list[dict[str, Any]]]:
        """Connects to the Socrata API using the provided app token and retrieves COT
        data filtered by market and exchange names.

        Params:
            app_token (str): The Socrata API application token for authentication.
                Required for API access. Get one from https://dev.socrata.com/
            market_and_exchange_names (list[str]): list of market and exchange names
                to filter the results. Use exact names as they appear in the CFTC database.
                Examples: ["EURO FX - CHICAGO MERCANTILE EXCHANGE", "CRUDE OIL, LIGHT SWEET - NEW YORK MERCANTILE EXCHANGE"]
            limit (int, optional): Maximum number of records to retrieve per market. 
                Defaults to 1000. API typically allows up to 50,000 records per request.
                Consider pagination for larger datasets to avoid timeout issues.
            order (str, optional): Sort order for results. Defaults to DESC_ORDER.
                (descending by date - most recent first).
                Use constants from cot_downloader.constants module for consistency.
        
        Returns:
            dict: A dictionary mapping each market name to its 
            corresponding COT report data. Each report is represented as a dictionary
            containing fields such as:
            - 'report_date_as_yyyy_mm_dd': Report publication date
            - 'market_and_exchange_names': Market identifier
            - 'commercial_long': Commercial trader long positions
            - 'commercial_short': Commercial trader short positions
            - Additional position and commitment data fields
            
            Returns empty dictionary if no market names are provided.
        
        Raises:
            ConnectionError: When the API request fails due to network issues,
                authentication problems, invalid parameters, or API errors.
                The original exception is chained for debugging purposes.
        
        Example:
            Download recent EURO FX reports:
            
            >>> reports = COTDownloader.download(
            ...     app_token="XWFI5HyH7penFtCH2bDwNR1JV",
            ...     market_and_exchange_names=["EURO FX - CHICAGO MERCANTILE EXCHANGE"],
            ...     limit=5
            ... )
            >>> 
            >>> # Access reports for specific market
            >>> euro_reports = reports["EURO FX - CHICAGO MERCANTILE EXCHANGE"]
            >>> for report in euro_reports:
            ...     print(f"Date: {report.get('report_date_as_yyyy_mm_dd')}")
            ...     print(f"Commercial Long: {report.get('commercial_long')}")
        """
        all_reports = {}
        if not market_and_exchange_names:
            return all_reports
        try:
            with Socrata(DOMAIN, app_token) as client:
                for name in market_and_exchange_names:
                    where_clause = COTDownloader._build_where_clause([name])
                    reports = client.get(
                        LEGACY_FUTUTRES_ONLY, 
                        where=where_clause,
                        limit = limit,
                        order = order
                    )
                    all_reports[name] = reports
            return all_reports
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
