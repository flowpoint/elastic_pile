#!/bin/python
import os
import sys
import logging
from logging import getLogger, StreamHandler, DEBUG
import re
import json

from time import time
from datetime import datetime
from itertools import chain, cycle, takewhile
import subprocess
from multiprocessing import pool, Pool, Queue
from typing import List, Tuple, Dict, Generator, Iterator, Sequence, Optional
from timeit import timeit

from more_itertools import chunked, flatten, sliced
from tqdm import tqdm # type: ignore

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, streaming_bulk, parallel_bulk
from elasticsearch_dsl import Search, Q

from tokenizers import pre_tokenizers, decoders, normalizers # type: ignore

patt = re.compile("[A-Z]?[a-z]*")

class ElasticHelper:
    def __init__(
            self,
            hosts,
            index_name,
            logger=None,
            bulk_request_size=1000,
            overwrite_index=False,
            timeout=30,
            ):
        '''usage:

        '''

        self.hosts = hosts
        self.es = Elasticsearch(self.hosts, timeout=timeout)
        self.index_name = index_name
        self.overwrite_index = overwrite_index
        self.logger = logger
        self.bulk_request_size = bulk_request_size

        if self.overwrite_index:
            self.logger.warning(f"overwriting index is set to True")

    def expand_relation(self, relation: str):
        return " ".join(patt.findall(relation)).lower()

    def streaming_bulk(self, processed: Iterator[dict]):
        if self.overwrite_index:
            if self.logger is not None:
                self.logger.warning(f"overwriting index is set to True")
                self.logger.warning(f"overwriting old index: {self.index_name}")
            self.es.indices.delete(index=self.index_name, ignore=[400, 404])

        if not self.es.indices.exists(index=self.index_name):
            if self.logger is not None:
                self.logger.info(f"creating new index: {self.index_name}")
            self._create_index()

        return streaming_bulk(self.es, processed, chunk_size=self.bulk_request_size)

    def refresh(self):
        self.es.indices.refresh()

    def _search(self, num_results=1, explain=False):
        s = Search(using=self.es, index=self.index_name) \
                .extra(track_total_hits=False, size=num_results, explain=explain)
        return s

    def search_word(self, word: str, **kwargs):
        s = self._search(**kwargs)

        q = Q('match', text=word)
        response = s.query(q).execute()
        return response

    def search_triple(self, entityA: str, relation: str, entityB: str, **kwargs):
        s = self._search(**kwargs)

        qA = Q('match', text=entityA)
        qR = Q('match', text=self.expand_relation(relation))
        qB = Q('match', text=entityB)
        q1 = (qA & qB)
        q = q1 | (q1 & qR)

        response = s.query(q).execute()
        return response

    def search_doc_not_entityB(self, doc: str, entityB, **kwargs):
        s = self._search(**kwargs)

        q1 = Q('match', text=doc, cutoff_frequncy=10000)
        q2 = ~Q('match', text=entityB)
        q = q1 & q2

        response = s.query(q).execute()
        return response

    def search_phrase(self, entityA: str, entityB: str, num_results: int = 10, max_slop: int = 20):
        query = {
                "size": num_results,
                "query": {
                    "bool": {
                        "must": {
                            "match_phrase": {
                                "text": {
                                    "query": f"{entityA} {entityB}",
                                    "slop": max_slop,
                                    },
                                },
                            },
                        }
                    },
                "highlight": {
                    "fields": {
                        "text": {
                            "type": "unified",
                            "max_analyzed_offset": 1000000 - 1,
                            },
                        },
                    },
                }
        res = self.es.search(index=self.index_name, body=query)
        return res

    def _create_index(self):
        self.es.indices.create(
            index=self.index_name,
            body={
                "settings": {
                    "refresh_interval": "30s",
                    "number_of_replicas": 1,
                    },
                "mappings": {
                    "dynamic": False,
                    "properties": {
                        "text": {
                            "type": "text",
                            "norms": True,
                            "similarity": "BM25",
                            "index_options": "positions",
                        },
                    }
                },

            },
            ignore=400,
        )

    def result_to_list(self, res):
        l = []
        for hit in res['hits']['hits']:
            l.append(hit["_source"])

        return l


