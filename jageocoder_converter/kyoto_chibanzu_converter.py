import csv
from logging import getLogger
import os
from typing import Union, Optional, List
import urllib.request

from jageocoder.address import AddressLevel
from jageocoder.node import AddressNode

from jageocoder_converter.base_converter import BaseConverter
from jageocoder_converter.data_manager import DataManager
import zipfile
import pandas as pd
import shapefile


from shapely.geometry import shape

logger = getLogger(__name__)


def get_polygon_centroid(geojson_geometry):
    polygon = shape(geojson_geometry)

    if polygon.area > 0:
        centroid = polygon.centroid
        return centroid.x, centroid.y
    else:
        return None

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
    def process_line(self, aza_code, ku_name, town_name, chiban, centroid):
        """
        add an address node.
        """
        lng, lat = centroid
        print(aza_code, ku_name, town_name, chiban, lng, lat)

    def convert(self):
        """
        Read records from zipped shapefile './data.zip', format them,
        then output to 'output/xx_kyoto_chibanzu.txt'.
        """
        self.prepare_jiscode_table()

        output_filepath = os.path.join(
            self.output_dir, '{}_kyoto_chibanzu.txt'.format(26))
        if os.path.exists(output_filepath):
            logger.info("SKIP: {}".format(output_filepath))
            return

        target_shp_entry = os.path.join(self.input_dir, 'data.shp')

        input_zipfilepath = os.path.join(self.input_dir, 'data.zip')
        if not os.path.exists(input_zipfilepath):
            self.download_files()

        # # unzip
        source_shp_entry = None
        excel_entry = None
        with zipfile.ZipFile(input_zipfilepath, 'r', metadata_encoding="CP932") as z:
          for filename in z.namelist():
              if filename.endswith('.shp'):
                source_shp_entry = os.path.join(self.input_dir, filename)
              elif filename.endswith('.xlsx'):
                excel_entry = os.path.join(self.input_dir, filename)
              z.extract(filename, self.input_dir)

        if source_shp_entry == None:
            raise RuntimeError("No .shp file found in {}".format(input_zipfilepath))
        if excel_entry == None:
            raise RuntimeError("No .xlsx file found in {}".format(input_zipfilepath))

        # 町名一覧 excel to Map
        town_map = {}
        df = pd.read_excel(excel_entry, sheet_name='町名一覧', header=0)
        for _, row in df.iterrows():
            aza_code = row['字CD']
            town_name = row['区名町名']
            ku_name = town_name.split('区')[0] + '区'
            town_name = town_name.split('区')[1]
            town_map[aza_code] = {
                'ku': ku_name,
                'town': town_name
            }

        command = 'ogr2ogr -t_srs EPSG:4326 -oo ENCODING=shift_jis {} {}'.format(target_shp_entry, source_shp_entry)
        os.system(command)

        # convert
        with shapefile.Reader(target_shp_entry) as shp:
            for shprec in shp.iterShapeRecords():
              shp = shprec.shape
              rec = shprec.record
              if shp.shapeTypeName == 'POLYGON':
                aza_code = rec[0]
                chiban = rec[1]
                if aza_code in town_map:
                  geom = shp.__geo_interface__
                  polygon_centroid = get_polygon_centroid(geom)
                  ku_name = town_map[aza_code]['ku']
                  town_name = town_map[aza_code]['town']
                  self.process_line(aza_code, ku_name, town_name, chiban, polygon_centroid)

    def download_files(self):
        """
        Download zipped data files from
        '京都市固定資産税（土地）地番図データ'
        https://data.city.kyoto.lg.jp/dataset/00652/
        """

        basename = 'data.zip'
        filename = os.path.join(self.input_dir, basename)
        if os.path.exists(filename):
          logger.info("'{}' exists. (skip downloading)".format(filename))
          return
        else:
          os.makedirs(self.input_dir, exist_ok=True)

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
            with open(filename, 'wb') as file:
                file.write(response.read())
                print(f"File downloaded and saved as {filename}")
