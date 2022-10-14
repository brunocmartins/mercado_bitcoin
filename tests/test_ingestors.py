import datetime
from unittest.mock import mock_open, patch

import pytest

from mercado_bitcoin.ingestors import DataIngestor
from mercado_bitcoin.writers import DataWriter


@pytest.fixture
@patch("mercado_bitcoin.ingestors.DataIngestor.__abstractmethods__", set())
def data_ingestor_fixture():
    return DataIngestor(
        writer=DataWriter,
        coins=["TEST", "BCM"],
        default_start_date=datetime.datetime(2022, 10, 10),
    )


@patch("mercado_bitcoin.ingestors.DataIngestor.__abstractmethods__", set())
class TestIngestors:
    def test_checkpoint_filename(self, data_ingestor_fixture):
        actual = data_ingestor_fixture._checkpoint_filename
        expected = "DataIngestor.checkpoint"

        assert actual == expected

    def test_load_checkpoint_no_checkpoint(self, data_ingestor_fixture):
        actual = data_ingestor_fixture._load_checkpoint()
        expected = datetime.datetime(2022, 10, 10)

        assert actual == expected

    @patch("builtins.open", new_callable=mock_open, read_data="2022-10-5")
    def test_load_checkpoint_existing_checkpoint(self, mock, data_ingestor_fixture):
        actual = data_ingestor_fixture._load_checkpoint()
        expected = datetime.date(2022, 10, 5)

        assert actual == expected

    @patch('mercado_bitcoin.ingestors.DataIngestor._load_checkpoint', return_value=datetime.date(2022, 1, 1))
    def test_get_checkpoint_file_exists(self, mock_load_checkpoint):
        data_ingestor = DataIngestor(
            writer=DataWriter,
            coins=["TEST", "BCM"],
            default_start_date=datetime.datetime(2022, 10, 10),
        )
        actual = data_ingestor._get_checkpoint()
        expected = datetime.date(2022, 1, 1)

        assert actual == expected
    
    @patch('mercado_bitcoin.ingestors.DataIngestor._load_checkpoint', return_value=None)
    def test_get_checkpoint_file_doesnt_exist(self, mock_load_checkpoint):
        data_ingestor = DataIngestor(
            writer=DataWriter,
            coins=["TEST", "BCM"],
            default_start_date=datetime.datetime(2022, 10, 10),
        )
        actual = data_ingestor._get_checkpoint()
        expected = data_ingestor.default_start_date

        assert actual == expected

    @patch("mercado_bitcoin.ingestors.DataIngestor._write_checkpoint", return_value=None)
    def test_update_checkpoint_checkpoint_updated(self, mock, data_ingestor_fixture):
        data_ingestor = data_ingestor_fixture
        data_ingestor._update_checkpoint(value=datetime.date(2022, 1, 1))
        actual = data_ingestor._checkpoint
        expected = datetime.date(2022, 1, 1)

        assert actual == expected

    @patch("mercado_bitcoin.ingestors.DataIngestor._write_checkpoint", return_value=None)
    def test_update_checkpoint_checkpoint_written(self, mock, data_ingestor_fixture):
        data_ingestor = data_ingestor_fixture
        data_ingestor._update_checkpoint(value=datetime.date(2022, 1, 1))

        mock.assert_called_once()

    @patch("builtins.open", new_callable=mock_open, read_data="2022-10-10")
    @patch(
        "mercado_bitcoin.ingestors.DataIngestor._checkpoint_filename",
        return_value="foobar_filename",
    )
    def test_write_checkpoint(
        self, mock_checkpoint_filename, mock_open_file, data_ingestor_fixture
    ):
        data_ingestor = data_ingestor_fixture
        data_ingestor._write_checkpoint()

        mock_open_file.assert_called_with(mock_checkpoint_filename, "w")
