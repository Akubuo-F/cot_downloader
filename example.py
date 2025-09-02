from cot_downloader.cot_downloader import COTDownloader


def example():
    market_and_exchange_names = [
        "EURO FX - CHICAGO MERCANTILE EXCHANGE", 
        "AUSTRALIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE"
    ]
    reports = COTDownloader.download(
        app_token="YOUR-APP-TOKEN",
        market_and_exchange_names=market_and_exchange_names,
        limit=len(market_and_exchange_names)
    )
    for report in reports:
        print(report)
        print()


if __name__ == "__main__":
    example()