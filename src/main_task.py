import json
import os

import luigi
from luigi import build

from src.config.common import PARENT_FOLDER
from src.extract import extract_data
from src.load import load_data
from src.transform import transform_data


class ExtractTask(luigi.Task):
    extract_file_mapping = luigi.Parameter()

    def output(self):
        file_mapping_dict = json.loads(extract_file_mapping)
        return {csv_name: luigi.LocalTarget(os.path.join(PARENT_FOLDER, "data", "extract", csv_name)) for _, csv_name in
                file_mapping_dict.items()}

    def run(self):
        file_mapping_dict = json.loads(extract_file_mapping)
        extract_data(file_mapping_dict)


class LoadTask(luigi.Task):
    extract_file_mapping = luigi.Parameter()
    load_file_mapping = luigi.Parameter()

    def requires(self):
        return ExtractTask(extract_file_mapping=self.extract_file_mapping)

    def output(self):
        return luigi.LocalTarget(os.path.join(PARENT_FOLDER, "data", "load", "status.csv"))

    def run(self):
        file_mapping_dict = json.loads(load_file_mapping)
        load_data(file_mapping_dict)


class TransformTask(luigi.Task):
    extract_file_mapping = luigi.Parameter()
    load_file_mapping = luigi.Parameter()

    def requires(self):
        return LoadTask(extract_file_mapping=self.extract_file_mapping, load_file_mapping=self.load_file_mapping)

    def output(self):
        return luigi.LocalTarget(os.path.join(PARENT_FOLDER, "data", "transform", "status.csv"))

    def run(self):
        transform_data()


if __name__ == '__main__':
    extract_file_mapping = json.dumps({
        'customers.sql': 'dim_customer.csv',
        'products.sql': 'dim_product.csv',
        'sellers.sql': 'dim_seller.csv'
    })

    load_file_mapping = json.dumps({
        'dim_customer.csv': 'dim_customer',
        'dim_product.csv': 'dim_product',
        'dim_seller.csv': 'dim_seller'
    })

    build(
        [TransformTask(extract_file_mapping=extract_file_mapping, load_file_mapping=load_file_mapping)],
        local_scheduler=True)
