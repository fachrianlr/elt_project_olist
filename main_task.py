# elt_pipeline.py
import luigi
import pandas as pd


class ExtractTask(luigi.Task):
    """
    Extract Task: Reads data from a simulated source (CSV file)
    """

    def output(self):
        # Define the output location (this will be used as the next step's input)
        return luigi.LocalTarget('data/source.csv')

    def run(self):
        # Simulating extract.py step (usually, you'd extract.py data from an external source)
        data = {
            'id': [1, 2, 3],
            'name': ['John', 'Jane', 'Doe'],
            'sales': [200, 450, 300]
        }
        df = pd.DataFrame(data)
        df.to_csv(self.output().path, index=False)


class LoadTask(luigi.Task):
    """
    Load Task: Loads extracted data to another location
    """

    def requires(self):
        return ExtractTask()

    def output(self):
        # Define the output location where the data will be loaded
        return luigi.LocalTarget('data/loaded.csv')

    def run(self):
        # Load the extracted data (from the CSV file) and save it to a new location
        df = pd.read_csv(self.input().path)
        df.to_csv(self.output().path, index=False)


class TransformTask(luigi.Task):
    """
    Transform Task: Applies transformation to the loaded data
    """

    def requires(self):
        return LoadTask()

    def output(self):
        # Define the output location where the transformed data will be saved
        return luigi.LocalTarget('transformed.csv')

    def run(self):
        # Read the loaded data
        df = pd.read_csv(self.input().path)

        # Example transformation: Add a new column that increases sales by 10%
        df['sales_with_bonus'] = df['sales'] * 1.10

        # Save the transformed data
        df.to_csv(self.output().path, index=False)


if __name__ == '__main__':
    # Run the TransformTask, which depends on the LoadTask and ExtractTask
    luigi.run(['TransformTask', '--local-scheduler'])

