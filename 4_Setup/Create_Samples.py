import secrets
import re


class gds:
    def __init__(self):
        """Initializes the Connection"""
        from google.cloud import datastore
        from google.auth import compute_engine
        global credentials
        global client
        credentials = compute_engine.Credentials()
        self.client = datastore.Client.from_service_account_json(
            'sa.json')

    def insert(self, str_articlenumber, str_URL, str_title, str_author,
               str_pubdate, str_text, int_claps, Tag_list, int_responses):
        """ Inserts one sample-entry"""

        from google.cloud import datastore

        Article = datastore.Entity(
            self.client.key('Article_ID', str_articlenumber),
            exclude_from_indexes=['Text'])

        Article.update({
            "URL": str_URL,
            "Title": str_title,
            "Author": str_author,
            "PublishingDate": str_pubdate,
            "Text": str_text,
            "Claps": int_claps,
            "Reading_time": 1,
            "Tags": Tag_list,
            "No_Responses": int_responses
        })
        self.client.put(Article)


# Init Class
g = gds()

# This Function creates 20 Sampleentries
for x in range(0, 20):
    Articlenumber = secrets.token_hex(12)
    URL = "http://sampleurl.com/?t=" + secrets.token_hex(12)
    Title = "A Title will be processed with the text"
    Author = "Juergen Schmidl"
    PublishingDate = "2020-01-23"
    text = '''1. Prerequisites to the readerThe project was developed on the
     Google Cloud Platform and it is recommended to do the tutorial there as
     well. Nevertheless you can run it on a local machine, but you need to
     alter code and replace some the used resources.This article is aimed at
     readers who already have some experience with the Google Cloud Platform
     and Linux shell. To help new readers getting started, links to additional
     resources can be found within this article.If you haven't worked with
     Google Cloud Plattform, you can take advantage of Google's free trial
     program:2. Introduction 2.1 Purpose of this ProjectThe goal of this
     article is to show how entities (e.g. technologies) can be extracted
     from articles (based on the structure of TowardsDatascience) in
     a scalable way using NLP. We will also look at how we can use other
     NLP methods like POS tagging.2.2 Introduction to scalable NLP Natural
     Languare Processing (NLP)Natural Language Processing describes techniques
     and methods for processing and analysis of natural language by machines.
     In this article we will try to extract technologies (e.g. Docker, Hadoop,
     Python, etc.) from (dummy) text.Why should it be scalable?The processing
     of written language can be very complex and can take a long time without
     scalable architecture. Of course you can upgrade a system and use faster
     processors, but the costs increase disproportionately to the achieved
     efficiency gains. It is better to choose an architecture from the
     beginning that can distribute the computing load over several machines.
     Apache SparkSpark is a great way to make data processing and machine
     learning scalable. It can run locally or on a cluster. Spark uses
     distributed Datasets, as well as processing pipelines. Unfortunately,
     in this article we can not go into Spark in detail, further information
     can be found e.g. here:Introduction to Apache SparkMapReduce and Spark
     are both used for large-scale data processing. However, MapReduce has
     some shortcomings which…towardsdatascience.comSpark-NLPSpark-NLP is a
     library for Python and Skala, which allows to process written language
     with Spark. I will go through it in the following chapters. More
     information can be found here:Introduction to Spark NLP: Foundations
     and Basic ComponentsWhy would we need another NLP library?
     towardsdatascience.com RedisRedis is a key value store that we will
     use to build a task queue.Docker and KubernetesA Docker container can
     be imagined as a complete system in a box. If the code runs in a
     container, it is independent from the hosts operating system. These
     properties limit the scalability of Spark, but can be compensated by
     using a Kubernetes cluster. Those clusters scale over the number of
     running Docker containers. If you want to know more, please see:
     Kubernetes? Docker? What is the difference?From a distance, Docker
     and Kubernetes can appear to be similar technologies; they both help you
     run applications…blog.containership.io ArchitectureBut first of all'''
    Claps = 0
    Tags = ["Tag1, Tag2, Tag3"]
    No_Responses = 0

    # Remove special Chars to increase the schedulers performance
    # see 6.3
    text = (" ".join(re.findall(r"[A-Za-z0-9]*", text))).replace("  ", " ")

    g.insert(Articlenumber, URL, Title, Author, PublishingDate, text,
             Claps, Tags, No_Responses,)
print("Samples generated")
