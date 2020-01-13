import redis
import re
import os


class RedisQueue(object):
    """Simple Queue with Redis Backend"""
    # From Peter Hoffmann
    # Visit:http://peter-hoffmann.com/2012/python-simple-queue-redis-queue.html

    def __init__(self, name, namespace='queue'):

        # If necessary you can specifiy here your own connection
        self.__db = redis.Redis(host="127.0.0.1", db=0,
                                socket_connect_timeout=2, socket_timeout=2,
                                encoding=u'utf-8')
        self.key = '%s:%s' % (namespace, name)
        self.__db.ping()
        print('connected to redis "{}"'.format("redis-server"))

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


class Scheduler():
    "Simple Job Scheduler for Redis"

    def __init__(self):
        """Initializes all necessary resources for the scheduler"""
        # - Google Cloud Datastore (Datasource)
        # - Redis Queue for job scheduling

        # Initialize GDS and BigQuery
        from google.cloud import datastore
        from google.cloud import bigquery

        # You can use different authentications, because of convinice i like
        # to use the credentials stored in the service account .json file.
        # How to get one:
        # https://cloud.google.com/iam/docs/creating-managing-service-account-keys?hl=en

        # Specify your Google Serivce Account
        # typicaly it should look like this: twds-project-91*******c2.json
        # I renamed it:

        self.project = os.getenv('Project')
        self.sa = 'sa.json'
        self.client = datastore.Client.from_service_account_json(self.sa)

        if self.project == "yourproject":
            print("It seems you havent configured the ProjectID")

        # Set Cursor for batch extraction None = start from the beginning
        self.next_cursor = None

        # Redis Queue initialisieren
        self.q = RedisQueue('Explain_Jobs')

        # Init for BigQuery (Just for transfer of master-data)
        # If you dont want to recreate the whole Project, you can delete them
        # and the correspondig Functions:
        # - create_Masterdata_for_Article
        # - create_Tags_for_Article

        # Get BigQuery Client
        self.BQclient = bigquery.Client.from_service_account_json(self.sa)

        # Set Dataset
        dataset_id = 'tbl_entity'  # Specify your Dataset ID

        # Set Tablenames
        table_id_master = 'Article_masterdata'  # Specify your Table ID
        table_id_tags = 'Article_tags'

        # Get Table refs
        table_ref_master = self.BQclient.dataset(
            dataset_id).table(table_id_master)

        table_ref_tags = self.BQclient.dataset(
            dataset_id).table(table_id_tags)

        # Set Tables
        self.table_Masterdata = self.BQclient.get_table(table_ref_master)
        self.table_tags = self.BQclient.get_table(table_ref_tags)

        # To prevent multiple Entries, we get a list of processed Articles
        # Set Query
        processed_entities = self.BQclient.query(
            "SELECT ID FROM `" + self.project +
            ".tbl_entity.Article_masterdata`")

        # Create empty list to store the results
        self.all_processed_entities = []

        # Fill list
        for row in processed_entities.result():
            self.all_processed_entities.append(row[0])

        print("Initialized")

    def get_next_Batch(self, Batch_size=10):
        """ Read Cloud Datastore batchwise"""
        # You can specify larger Batches to increase performace

        # Set up Query
        query = self.client.query(kind='Article_ID')

        # Execute Query and pass Cursor
        query_i = query.fetch(start_cursor=self.next_cursor, limit=Batch_size)
        batch = next(query_i.pages)

        # Extract Articles out of batch
        Articles = list(batch)

        # set Cursor for next Execution
        self.next_cursor = query_i.next_page_token

        # return result as list
        return Articles

    def Send_Job(self, Text, ID):
        """Send ID and String to Redis-Server"""

        # Redis doesn't allow special characters, so we need to replace them
        # Regex is quite slow, so we check first if its needed
        if Text.replace(" ", "").isalnum() is False:
            Text = (" ".join(re.findall(r"[A-Za-z0-9]*", Text))).replace(
                "  ", " ")

        # Since we only store strings, the ID is separated with a "/""
        self.q.put(ID + "/" + Text)

    def Process_Batch(self):
        """ Starts the Scheduler """

        # set Variable for Loop
        loop = True

        # start Loop
        while loop is True:

            # Get Batch of Articles
            Batch = self.get_next_Batch(10)

            # Iterate trough Batch
            for Article in Batch:

                # Check if Article has already been processed
                if Article.key.id_or_name not in self.all_processed_entities:

                    # Combine and send article to redis
                    self.Send_Job(Article['Title'] + " " +
                                  Article['Text'], str(Article.key.id_or_name))

                    # Create Masterdata for Article
                    self.create_Masterdata_for_Article(Article)
                else:
                    print("Article allready processed")
                print("Queue size: " + str(self.q.qsize()))

                # Check if cursor has reached EOF (End of File)
            if self.next_cursor is None:
                loop = False
                print("No more entires")

    def create_Masterdata_for_Article(self, Article):
        """This Function creates Materdata in BigQuery for the Article"""
        # This Function is needed to transfer data for Part3 of this Project

        # Build Row for Bigquery Insert of Masterdata
        try:
            row = [(Article.key.id_or_name,
                    Article["PublishingDate"],
                    Article["Author"],
                    Article["Title"],
                    Article["Claps"],
                    Article["No_Responses"],
                    Article["Reading_time"])]

            # Write the Tags of the Article at a different Table
            self.create_Tags_for_Article(Article)

            # Write Masterdata
            errors = self.BQclient.insert_rows(self.table_Masterdata, row)

            # Check, if any Error occoured
            if errors != []:
                print(errors)

            # Fail if an Error has occoured
            assert errors == []

        except BaseException:
            print("Error parsing Masterdata for Article " +
                  str(Article.key.id_or_name))

    def create_Tags_for_Article(self, Article):
        """This Function creates additional Materdata for the Article"""
        # This Function is needed to transfer data for Part3 of this Project

        rows = []
        for item in Article["Tags"]:
            # Append Tags to push them to BigQuery
            rows.append((Article.key.id_or_name, item))

        # Insert row
        errors = self.BQclient.insert_rows(self.table_tags, rows)

        # Check, if any Error occoured
        assert errors == []


if __name__ == '__main__':  # Does only execute if main script
    print("Start NLP Scheduler")
    s = Scheduler()
    s.Process_Batch()
    print("NLP Scheduler finished")
