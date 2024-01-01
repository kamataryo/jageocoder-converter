import csv
from logging import getLogger
import os
from typing import Union, Optional, List
import urllib.request

from jageocoder.address import AddressLevel
from jageocoder.node import AddressNode

from jageocoder_converter.base_converter import BaseConverter
from jageocoder_converter.data_manager import DataManager

logger = getLogger(__name__)


class KyotoChibanzuConverter(BaseConverter):
    """
    A converter to generate formatted text data of Chiban
    from kyoto city chiban data.
    https://data.city.kyoto.lg.jp/dataset/00652/

    Output 'output/xx_kyoto_chiban.txt' for each prefecture.
    """
    dataset_name = "京都市固定資産税（土地）地番図データ"
    dataset_url = "https://data.city.kyoto.lg.jp/dataset/00652/"

    def __init__(self,
                 output_dir: Union[str, bytes, os.PathLike],
                 input_dir: Union[str, bytes, os.PathLike],
                 manager: Optional[DataManager] = None,
                 priority: Optional[int] = None,
                 targets: Optional[List[str]] = None,
                 quiet: Optional[bool] = False) -> None:
        super().__init__(
            manager=manager, priority=priority, targets=targets, quiet=quiet)
        self.output_dir = output_dir
        self.input_dir = input_dir
        self.fp = None

    def confirm(self) -> bool:
        """
        Show the terms of the license agreement and confirm acceptance.
        """
        terms = (
            "「京都市固定資産税（土地）地番図データ」をダウンロードします。\n"
            "https://data.city.kyoto.lg.jp/dataset/00652/ の"
            "説明およびライセンスを必ず確認してください。\n"
        )
        return super().confirm(terms)

    # TODO: not implemented
    def process_line(self, args):
        """
        Process a line of the input file.
        """

    # TODO: not implemented
    def add_from_csvfile(self, csvfilepath: str, pref_code: str):
        """
        Register address notations from Geolonia 住所データ
        for the pref represented by pref_code.
        """

    # TODO: not implemented
    def convert(self):
        """
        Read records from 'geolonia/latest.csv' file, format them,
        then output to 'output/xx_geolonia.txt'.
        """

    def download_files(self):
        """
        Download zipped data files from
        '京都市固定資産税（土地）地番図データ'
        https://data.city.kyoto.lg.jp/dataset/00652/
        """

        url = "https://data.city.kyoto.lg.jp/resource/?id=18537"
        headers = {'content-type': 'application/x-www-form-urlencoded'}
        data = {
            'upload_file': '20231130081822_%E4%BA%AC%E9%83%BD%E5%B8%82%E5%9B%BA%E5%AE%9A%E8%B3%87%E7%94%A3%E7%A8%8E%EF%BC%88%E5%9C%9F%E5%9C%B0%EF%BC%89%E5%9C%B0%E7%95%AA%E5%9B%B3%E3%83%87%E3%83%BC%E3%82%BFR05.zip',
            'upload_url': '',
            'download': '%E3%81%93%E3%81%AE%E3%83%87%E3%83%BC%E3%82%BF%E3%82%92%E3%83%80%E3%82%A6%E3%83%B3%E3%83%AD%E3%83%BC%E3%83%89'
        }

        form_data = []
        for key, value in data.items():
            form_data.append(key + '=' + value)

        encoded_data = '&'.join(form_data).encode('ascii')
        request = urllib.request.Request(url, data=encoded_data, headers=headers, method='POST')

        with urllib.request.urlopen(request) as response:
            basename = 'data.zip'
            filename = os.path.join(self.input_dir, basename)

            if os.path.exists(filename):
                logger.info("SKIP: {}".format(filename))
            else:
                os.makedirs(self.input_dir, exist_ok=True)

            with open(filename, 'wb') as file:
                file.write(response.read())
                print(f"File downloaded and saved as {filename}")
