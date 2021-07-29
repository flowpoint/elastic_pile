import unittest
import os
from itertools import chain

from more_itertools import flatten
from lm_dataformat import Reader, Archive

from indexer import Chunker

import json


def test_example_from_jsonl(input_filename: str, output_filename: str):
    rdr = Reader(input_filename)

    ar = Archive(output_filename)

    for k, doc in enumerate(rdr.stream_data()):
        #print(doc)
        ar.add_data({"text":doc})

        if k > 100:
            break

    #ar.commit(archive_name="./tmp/testdata.jsonl.zst")
    ar.commit()


class IndexerTestCase(unittest.TestCase):
    def setUp(self):
        # test_example_from_jsonl("pile/val.jsonl.zst", "./tmp/testdata")
        # os.rename("./tmp/testdata/filename", "./tmp/testdata.jsonl.zst")

        path = ("./tmp/testdata.jsonl.zst")
        if not os.path.isfile(path):
            raise FileNotFoundError("testdata.jsonl.zst file missing")

        self.docs = []

        rdr = Reader("./tmp/testdata.jsonl.zst")
        for doc in rdr.stream_data():
            self.docs.append(doc)

        self.chunker = Chunker()


    def tearDown(self):
        pass

    def test_pretok_consistency(self):
        doc = str(self.docs[0])
        ndoc = self.chunker._norm(doc)
        enc_ndoc = self.chunker._encode(ndoc)
        dec_ndoc = self.chunker._decode(enc_ndoc)

        self.assertEqual(
                dec_ndoc,
                ndoc,
                )


if __name__ == '__main__':
    unittest.main()
