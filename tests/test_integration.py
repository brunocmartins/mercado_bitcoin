import datetime

from mercado_bitcoin.apis import DaySummaryApi


class TestDaySummaryApi:
    def test_get_data(self):
        actual = DaySummaryApi(coin="BTC").get_data(date=datetime.date(2022, 10, 10))
        expected = {
            "date": "2022-10-10",
            "opening": 102068.74932427,
            "closing": 99560.51242297,
            "lowest": 99000,
            "highest": 102102.98186431,
            "volume": "5602642.71890693",
            "quantity": "55.78294625",
            "amount": 4220,
            "avg_price": 100436.4791669,
        }

        assert actual == expected

    def test_get_data_better(self):
        actual = DaySummaryApi(coin="BTC").get_data(date=datetime.date(2022, 10, 10)).get('date')
        expected = "2022-10-10"

        assert actual == expected
