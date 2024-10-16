import json

import luigi
import pandas as pd
from luigi import build

from src.extract import extract_data


class ExtractTask(luigi.Task):
    file_mapping = luigi.Parameter()

    def output(self):
        # return luigi.LocalTarget('data/load.csv')
        pass

    def run(self):
        file_mapping_dict = json.loads(file_mapping)
        extract_data(file_mapping_dict)


class LoadTask(luigi.Task):

    def requires(self):
        return ExtractTask()

    def output(self):
        return luigi.LocalTarget('data/loaded.csv')

    def run(self):
        df = pd.read_csv(self.input().path)
        df.to_csv(self.output().path, index=False)


class TransformTask(luigi.Task):

    def requires(self):
        return LoadTask()

    def output(self):
        return luigi.LocalTarget('transformed.csv')

    def run(self):
        pass


if __name__ == '__main__':
    file_mapping = json.dumps({
        'customers.sql': 'dim_customer.csv',
        'products.sql': 'dim_product.csv',
        'sellers.sql': 'dim_seller.csv'
    })

    build(
        [ExtractTask(file_mapping=file_mapping)], local_scheduler=True)
