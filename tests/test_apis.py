import datetime
import pytest
from unittest.mock import patch

import requests

from mercado_bitcoin.apis import DaySummaryApi, MercadoBitcoinApi, TradesApi


class TestDaySummaryApi:
    @pytest.mark.parametrize(
        "coin, date, expected",
        [
            (
                "BTC",
                datetime.date(2022, 10, 10),
                "https://www.mercadobitcoin.net/api/BTC/day-summary/2022/10/10",
            ),
            (
                "ETH",
                datetime.date(2022, 10, 10),
                "https://www.mercadobitcoin.net/api/ETH/day-summary/2022/10/10",
            ),
            (
                "ETH",
                datetime.date(2021, 8, 6),
                "https://www.mercadobitcoin.net/api/ETH/day-summary/2021/8/6",
            ),
        ],
    )
    def test_get_endpoint(self, coin, date, expected):
        api = DaySummaryApi(coin=coin)
        actual = api._get_endpoint(date=date)

        assert actual == expected


class TestTradesApi:
    @pytest.mark.parametrize(
        "coin, date_from, date_to, expected",
        [
            (
                "TEST",
                datetime.datetime(2022, 1, 1),
                datetime.datetime(2022, 1, 2),
                "https://www.mercadobitcoin.net/api/TEST/trades/1641006000/1641092400",
            ),
            (
                "TEST",
                datetime.datetime(2022, 1, 1),
                None,
                "https://www.mercadobitcoin.net/api/TEST/trades/1641006000",
            ),
            (
                "TEST",
                None,
                datetime.datetime(2022, 1, 1),
                "https://www.mercadobitcoin.net/api/TEST/trades",
            ),
            ("TEST", None, None, "https://www.mercadobitcoin.net/api/TEST/trades"),
        ],
    )
    def test_get_endpoint(self, coin, date_from, date_to, expected):
        api = TradesApi(coin=coin)
        actual = api._get_endpoint(date_from=date_from, date_to=date_to)

        assert actual == expected

    def test_get_enpoint_date_from_greater_than_date_to(self):
        with pytest.raises(RuntimeError):
            TradesApi(coin="TEST")._get_endpoint(
                date_from=datetime.datetime(2022, 1, 2),
                date_to=datetime.datetime(2022, 1, 1),
            )

    @pytest.mark.parametrize(
        "date, expected",
        [
            (datetime.datetime(2022, 1, 1), 1641006000),
            (datetime.datetime(2022, 1, 2), 1641092400),
            (datetime.datetime(2022, 1, 2, 0, 0, 5), 1641092405),
        ],
    )
    def test_get_unix_epoch(self, date, expected):
        api = TradesApi(coin="TEST")
        actual = api._get_unix_epoch(date)

        assert actual == expected


@pytest.fixture()
@patch('mercado_bitcoin.apis.MercadoBitcoinApi.__abstractmethods__', set())
def mercado_bitcoin_api_fixture():
    return MercadoBitcoinApi(
            coin='TEST'
        )

def mocked_requests_get(*args, **kwargs):
    class MockResponse(requests.Response):
        def __init__(self, status_code, json_data):
            super().__init__()
            self.status_code = status_code
            self.json_data = json_data
        
        def raise_for_status(self):
            if self.status_code != 200:
                raise Exception

        def json(self):
            return self.json_data

    if args[0] == 'valid_endpoint':
        return MockResponse(json_data={'foo': 'bar'}, status_code=200)
    else:
        return MockResponse(json_data=None, status_code=404)


class TestMercadoBitcoinApi:

    @patch('requests.get')
    @patch('mercado_bitcoin.apis.MercadoBitcoinApi._get_endpoint', return_value='valid_endpoint')
    def test_get_data_requests_is_called(self, mock_get_endpoint, mock_requests, mercado_bitcoin_api_fixture):
        mercado_bitcoin_api_fixture.get_data()
        mock_requests.assert_called_once_with('valid_endpoint')

    @patch('requests.get', side_effect=mocked_requests_get)
    @patch('mercado_bitcoin.apis.MercadoBitcoinApi._get_endpoint', return_value='valid_endpoint')
    def test_get_data_with_valid_endpoint(self, mock_get_endpoint, mock_requests, mercado_bitcoin_api_fixture):
        actual = mercado_bitcoin_api_fixture.get_data()
        expected = {'foo': 'bar'}

        assert actual == expected

    @patch('requests.get', side_effect=mocked_requests_get)
    @patch('mercado_bitcoin.apis.MercadoBitcoinApi._get_endpoint', return_value='invalid_endpoint')
    def test_get_data_with_valid_endpoint(self, mock_get_endpoint, mock_requests, mercado_bitcoin_api_fixture):
        with pytest.raises(Exception):
            mercado_bitcoin_api_fixture.get_data()

    def test_cannot_instantiate_class_without_get_endpoint(self):
        class MercadoBitcoinApiWithoutGetEndpoint(MercadoBitcoinApi):
            pass

        with pytest.raises(TypeError):
            MercadoBitcoinApiWithoutGetEndpoint(coin='TEST')

    def test_can_instantiate_class_with_get_endpoint(self):
        class MercadoBitcoinApiWithGetEndpoint(MercadoBitcoinApi):
            def _get_endpoint(self):
                return 1
        actual = MercadoBitcoinApiWithGetEndpoint(coin='TEST')._get_endpoint()
        expected = 1

        assert actual == expected
