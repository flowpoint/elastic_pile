import unittest
import os
from itertools import chain

from more_itertools import flatten
from lm_dataformat import Reader, Archive

from string import printable, whitespace
from indexer import Chunker, Ingester, ElasticHelper
import random

from elasticsearch import Elasticsearch
import logging
from time import sleep

import json

class DummyLogger:
    def info(self, s):
        pass
    def warning(self, s):
        pass
    def error(self, s):
        pass


GOLDEN_PASSAGE = "hello, this is the golden document that tests for the world"
def test_example_from_jsonl(input_filename: str, output_filename: str):
    rdr = Reader(input_filename)

    ar = Archive(output_filename)

    for k, doc in enumerate(rdr.stream_data()):
        ar.add_data(doc)

        if k > 100:
            break

    ar.add_data(GOLDEN_PASSAGE)
    ar.commit()


class ChunkerTestCase(unittest.TestCase):
    def setUp(self):
        # test_example_from_jsonl("pile/val.jsonl.zst", "./tmp/testdata")
        # os.rename("./tmp/testdata/filename", "./tmp/testdata.jsonl.zst")

        self.tmpdir = "./tmp"
        self.path = "./tmp/testdata3.jsonl.zst"

        #self.logger = DummyLogger()
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

        if not os.path.isfile(self.path):
            raise FileNotFoundError("testdata.jsonl.zst file missing")

        self.docs = []

        rdr = Reader(self.path)
        for doc in rdr.stream_data():
            self.docs.append(doc)

        self.chunker = Chunker()

        random.seed(42)

        self.index_name = "throwaway_test_index"

        chars = "".join(set(printable) - set(whitespace))
        
        self.randomstr = "".join(random.choices(chars, k=10000))

        hosts = ["localhost"]
        self.es_helper = ElasticHelper(hosts, self.index_name, self.logger, overwrite_index=False)

        self.es_helper._create_index()


    def tearDown(self):
        self.es_helper.es.indices.delete(index=self.index_name, ignore=[400, 404])

    def test_chunker_consistency(self):
        doc = str(self.docs[0])
        ndoc = self.chunker._norm(doc)
        enc_ndoc, sus = self.chunker._encode(ndoc)
        dec_ndoc = self.chunker._decode(enc_ndoc)

        self.assertEqual(
                dec_ndoc,
                ndoc,
                )

    def test_chunker(self):
        doc = ""
        for i, c in enumerate(self.randomstr):
            doc += c
            if i % 6:
                doc += " "

        ndoc = self.chunker._norm(doc)
        enc_ndoc, sus = self.chunker._encode(ndoc)

        self.assertFalse(sus)

        dec_ndoc = self.chunker._decode(enc_ndoc)

        self.assertEqual(
                dec_ndoc,
                ndoc
                )

    def test_chunker_sus_documents(self):
        doc = self.randomstr 
        ndoc = self.chunker._norm(doc)
        enc_ndoc, sus = self.chunker._encode(ndoc)

        self.assertTrue(sus)

        dec_ndoc = self.chunker._decode(enc_ndoc)

        # the whitespaces don't match, but this is unimportant for the application
        self.assertEqual(
                dec_ndoc.replace(" ", ""),
                ndoc.replace(" ", ""),
                )

    def test_ingester_dryrun(self):
        id_ = 0
        
        ingester = Ingester(self.path, self.chunker, dryrun=True, es_helper=None, index_name=self.index_name, tmpdir=self.tmpdir, id_=id_, logger=self.logger)

        ingester.ingest(self.es_helper)

    def test_ingester_ingest(self):
        id_ = 0
        
        ingester = Ingester(self.path, self.chunker, dryrun=False, es_helper=self.es_helper, index_name=self.index_name, tmpdir=self.tmpdir, id_=id_, logger=self.logger)

        ingester.ingest(self.es_helper)
        self.es_helper.refresh()

        res = self.es_helper.search_phrase("hello", "world")
        hit = res["hits"]["hits"][0]["_source"]["text"]

        self.assertEqual(
                GOLDEN_PASSAGE,
                hit
                )


if __name__ == '__main__':
    unittest.main()
