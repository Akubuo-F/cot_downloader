from cot_downloader import COTDownloader


def example():
    result = COTDownloader.download(
        app_token="YOUR-APP-TOKEN",
        market_and_exchange_names=["EURO FX - CHICAGO MERCANTILE EXCHANGE", "AUSTRALIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE"],
        limit=2,
    )
    for reports in result.values():
        for report in reports:
            print(f"Date: {report.get('report_date_as_yyyy_mm_dd')}")
            print(f"Market: {report.get('market_and_exchange_names')}")



if __name__ == "__main__":
    example()