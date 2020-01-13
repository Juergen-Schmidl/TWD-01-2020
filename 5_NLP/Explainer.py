import redis
import os


class RedisQueue(object):
    """Simple Queue with Redis Backend"""
    # From Peter Hoffmann
    # Visit:http://peter-hoffmann.com/2012/python-simple-queue-redis-queue.html

    def __init__(self, name, namespace='queue'):
        # Those variables are provided by the redis-host
        REDIS_HOST = os.environ['REDISMASTER_SERVICE_HOST']
        REDIS_PORT = os.environ['REDISMASTER_SERVICE_PORT']

        self.__db = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0,
                                socket_connect_timeout=2, socket_timeout=2)
        self.key = '%s:%s' % (namespace, name)
        self.__db.ping()
        print('connected to redis "{}"'.format(REDIS_HOST))

    def qsize(self):
        """Return the approximate size of the queue."""
        return self.__db.llen(self.key)

    def empty(self):
        """Return True if the queue is empty, False otherwise."""
        return self.qsize() == 0

    def put(self, item):
        """Put item into the queue."""
        self.__db.rpush(self.key, item)

    def get(self, block=True, timeout=None):
        """Remove and return an item from the queue.

        If optional args block is true and timeout is None (the default), block
        if necessary until an item is available."""
        if block:
            item = self.__db.blpop(self.key, timeout=timeout)
        else:
            item = self.__db.lpop(self.key)

        if item:
            item = item[1]
        return item

    def get_nowait(self):
        """Equivalent to get(False)."""
        return self.get(False)


class SparkNLP_Explainer():
    """ Extracts Entities using Spark NLP"""

    def __init__(self):
        """Initialisation of this class"""
        print("Initialisierung")
        # Imports
        from os import path
        from pyspark.ml import PipelineModel
        from sparknlp import start
        from google.cloud import bigquery

        # Define Service-Account json
        self.sa = 'sa.json'

        # Initialisation of Spark
        print("Starting Spark")
        start()

        # Load pretrained Modell
        print("load Model")
        if path.exists("Model_Template") is False:
            print("Model needs to be loaded")
            self.load_Model()
            print("Model loaded")
        else:
            print("Found existing model")

        # loads downloaded Modell
        self.Model = PipelineModel.read().load("Model_Template")

        # Initialize BigQuery
        print("Init BigQuery Connection")

        # Get Client from JSON File (specified above)
        self.client = bigquery.Client.from_service_account_json(self.sa)

        dataset_id = 'tbl_entity'  # Specify your Dataset ID
        table_id = 'Entity_raw'  # Specify your Table ID
        table_ref = self.client.dataset(dataset_id).table(table_id)
        self.table = self.client.get_table(table_ref)  # request

        # Initialize Redis Queue
        self.q = RedisQueue('Explain_Jobs')
        print("Init done")

    def Explain(self, text, ID):
        """Does the NLP on the String and insert the result in BigQuery"""
        from sparknlp.base import LightPipeline

        print("Explain... " + ID)

        # In this case we use a Light-Pipeline. For furter information see:
        # https://medium.com/spark-nlp/spark-nlp-101-lightpipeline-a544e93f20f1
        # To set up the Pipeline the prevous downloaded Modell is needed

        lp = LightPipeline(self.Model)

        # No let the Pipeline annotate our text
        r = lp.annotate(text)

        # Create empty list
        toret = []

        # iterate through entities
        for i in r["entities"]:
            # and append it to the list with the corresponding ID
            toret.append((i, ID))

        # Check if list is not empty
        if toret != []:

            # Create for each item a row in BigQuery
            errors = self.client.insert_rows(self.table, toret)  # API request

            # Print possible errors
            if errors != []:
                print(errors)
        else:
            print("No entities found")

    def load_Model(self):
        """ Extracts the Pipelinemodel"""
        from os import remove
        from shutil import unpack_archive

        if os.path.exists("Model_Template.zip"):
            # extrakt files from archive
            unpack_archive("Model_Template.zip", "Model_Template", "zip")

            # Delete File
            remove("Model_Template.zip")
        else:
            print("No Model Template found!")


if __name__ == "__main__":

    # Create Instance of Class
    e = SparkNLP_Explainer()
    print("Worker has been started")
    while 1 == 1:  # Wait for work

        # Check if queue is empty
        if e.q.empty() is False:

            # Split String to get ID as well as text
            Job = str(e.q.get().decode("utf-8")).split("/")

            # Write string contents to variable
            ID = Job[0]
            print(ID)
            Text = Job[1]

            # Start Explainer
            e.Explain(Text, ID)
            print("Text has been successfully processed")
