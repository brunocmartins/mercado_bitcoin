from abc import ABC, abstractmethod
import datetime
import logging

from backoff import on_exception, expo
import ratelimit
import requests


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class MercadoBitcoinApi(ABC):
    def __init__(self, coin: str) -> None:
        self.coin = coin
        self.base_endpoint = "https://www.mercadobitcoin.net/api"

    @abstractmethod
    def _get_endpoint(self, **kwargs) -> str:
        """
        Add way to get the full API endpoint accordingly to its method
        """

    # Limits the amount of requests if there is more then 30 call to the API in less than 30 seconds
    # Raises a ratelimit error and waits to avoid API overload
    @on_exception(expo, ratelimit.exception.RateLimitException, max_tries=15)
    @ratelimit.limits(calls=30, period=30)
    @on_exception(expo, requests.exceptions.HTTPError, max_tries=10)
    def get_data(self, **kwargs) -> dict:
        endpoint = self._get_endpoint(**kwargs)
        logger.info(f"Getting data from endpoint: {endpoint}")
        response = requests.get(endpoint)
        response.raise_for_status()

        return response.json()


class DaySummaryApi(MercadoBitcoinApi):
    type_ = "day-summary"

    def _get_endpoint(self, date: datetime.date) -> str:
        return f"{self.base_endpoint}/{self.coin}/{self.type_}/{date.year}/{date.month}/{date.day}"


class TradesApi(MercadoBitcoinApi):
    type_ = "trades"

    def _get_unix_epoch(self, date: datetime) -> int:
        return int(date.timestamp())

    def _get_endpoint(
        self, date_from: datetime.datetime = None, date_to: datetime.datetime = None
    ) -> str:
        if date_from and not date_to:
            unix_date_from = self._get_unix_epoch(date_from)
            endpoint = f"{self.base_endpoint}/{self.coin}/{self.type_}/{unix_date_from}"
        elif date_from and date_to:
            if date_from > date_to:
                raise RuntimeError("date_from cannot be greater than date_to")
            unix_date_from = self._get_unix_epoch(date_from)
            unix_date_to = self._get_unix_epoch(date_to)
            endpoint = f"{self.base_endpoint}/{self.coin}/{self.type_}/{unix_date_from}/{unix_date_to}"
        else:
            endpoint = f"{self.base_endpoint}/{self.coin}/{self.type_}"

        return endpoint
