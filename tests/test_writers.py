from unittest.mock import mock_open, patch

import pytest

from mercado_bitcoin.writers import DataTypeNotSupportedForIngestionException, DataWriter


@pytest.fixture
def data_writer_fixture():
    return DataWriter(
        coin='TEST',
        api='test_api'
    )

class TestDataWriter:

    @patch('builtins.open', new_callable=mock_open, read_data=None)
    @patch('os.makedirs', return_value=None)
    def test_write_row(self, mock_makedirs, mock_open_file, data_writer_fixture):
        data_writer = data_writer_fixture
        data_writer._write_row('')

        mock_open_file.assert_called_with(data_writer.filename, 'a')

    @pytest.mark.parametrize(
        'data',
        [
            ('foobar'),
            (9),
            (1.1),
            (tuple(['foo', 'bar'])),
            (True),
            (None),
        ]
    )
    def test_write_wrong_datatype(self, data, data_writer_fixture):
        data_writer = data_writer_fixture
        with pytest.raises(DataTypeNotSupportedForIngestionException):
            data_writer.write(data)

    @patch('mercado_bitcoin.writers.DataWriter.write')
    def test_write_dict_datatype(self, mock_write, data_writer_fixture):
        data_writer = data_writer_fixture
        data_writer.write({'foo': 'bar'})
        mock_write.assert_called_once_with({'foo': 'bar'})

    @patch('mercado_bitcoin.writers.DataWriter._write_row')
    def test_write_list_datatype(self, mock_write, data_writer_fixture):
        data_writer = data_writer_fixture
        data_writer.write([{'foo': 'bar'}, {'bar': 'foo'}])

        assert mock_write.call_count == 2
