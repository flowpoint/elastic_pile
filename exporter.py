from logging import getLogger, StreamHandler, DEBUG
import re
import json
import os
from itertools import chain, cycle, takewhile
from timeit import timeit

from tqdm import tqdm
from more_itertools import chunked

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from neo4j import GraphDatabase
from n4 import KG

from elasticsearch_helper import ElasticHelper

handler = StreamHandler()
handler.setLevel(DEBUG)
getLogger("neo4j").addHandler(handler)

index_name = "pile_index"

#hosts = [f'http://elastic:{os.environ.get("ELASTIC_PASSWORD")}@localhost:9200']
hosts = "localhost"

es = ElasticHelper(hosts, index_name)

graph = KG("bolt://localhost:7687", "neo4j", "s3cr3t")


def rdfgen():
    for i, (s,r,o) in enumerate(graph.export()):
        s1 = s["properties"]["uri"].split("/")[-1].replace("_", " ")
        r1 = r['label'].split("/")[-1]
        o1 = o["properties"]["uri"].split("/")[-1].replace("_", " ")

        #s1 = s["properties"]["http://www.w3.org/2000/01/rdf-schema#label"].split("/").replace("_", " ")[-1]
        #o1 = o["properties"]["http://www.w3.org/2000/01/rdf-schema#label"].split("/").replace("_", " ")[-1]

        yield s1, r1, o1

def batch_to_bulk(x):
    return list(zip(cycle([{}]), x))

def to_query(x):
    s, r, o = x
    return search_w(s, r, o)

def se(qs):
    return es.msearch(body=chain(*qs), index=index_name)

def get_hits(x):
    #only the best hit for now
    hits = x["hits"]["hits"]
    if len(hits) == 0:
        return None
    else:
        return hits[0]['_source']['text']


def take(x, num):
    return map(lambda a: a[1], takewhile(lambda b: b[0] < num, enumerate(x)))

def save_to_file(a: str, fname: str):
    with open(f"samples/{fname}", "w") as f:
        f.write(a)


rdfg = rdfgen()
#rdfgs = map(lambda x: search_entities(*x), rdfg)
#rdfgs = map(lambda x: search_expanded_rdf(*x), rdfg)

#batches = list(take(chunked(rdfgs,5), 1))
rdfs = list(take(rdfgen(), 500))

#qs = list(map(lambda x: search_entities(*x), batches))

#bulk = batch_to_bulk(batches[0])

'''
res = se(bulk)

docs = ([get_hits(x) for x in res['responses']])

'''

negatives = []

testres = []

separator = "--------"

i = 0

for (s,r,o) in rdfs:
    #docA = es.search(index=index_name, body=search_word(s))
    #docB = es.search(index=index_name, body=search_word(o))
    docC = es.search(index=index_name, body=search_expanded_rdf(s,r,o))

    #doc1 = get_hits(docA)
    #doc2 = get_hits(docB)
    doc3 = get_hits(docC)

    if doc3 is not None:
        qqq = search_doc_not_entityB(doc3, s)
        #qqq = search_doc_not_doc(doc1, doc2, doc3)
        reee = es.search(index=index_name, body=qqq)

        nreee = get_hits(reee)

        negatives.append(nreee)
        testres.append(f"{separator}\ntuple:\n{s};{r};{o};\n{separator}\npositive:\n{doc3}\n{separator}\nnegative:\n{nreee}")
        #testres.append(f"{separator}\ntuple:\n{s};{r};{o};\n{separator}\npositive:\n{doc1}\n{separator}\npositive2:\n{doc2}\n{separator}\npositive3:\n{doc3}\n{separator}\nnegative:\n{nreee}")
        print(f"reee{i}")
        save_to_file(testres[-1], f"negative_{i}.txt")
    i += 1


#res2 = es.search(index=index_name, body=search_doc(docs[0]))

#graph.setup()

#graph.load_data()

#graph.setup()

#for rel in graph.export():
#    print(rel)

