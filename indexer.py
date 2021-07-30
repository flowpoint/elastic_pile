#!/bin/python
import os
import json
from itertools import accumulate, chain, starmap
from functools import reduce
from multiprocessing import pool, Pool, Queue
from typing import List, Tuple, Dict, Generator, Iterator

from more_itertools import chunked, flatten
from tqdm import tqdm # type: ignore

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, streaming_bulk, parallel_bulk
from tokenizers import pre_tokenizers, decoders, normalizers # type: ignore
from lm_dataformat import Reader # type: ignore

import cProfile, pstats, io
from pstats import SortKey

#mode = "dryrun"
#mode = "client"
mode = "index"

# the order of files is important, because we enumerate the id across files
filelist = sorted(["pile/val.jsonl.zst"])
#filelist = sorted(["../00.jsonl.zst"])

for f in filelist:
    if not os.path.isfile(f):
        raise FileNotFoundError(f)

index_name = "test_index"

# size in words according to pre_tokenizer
chunksize = 256
use_mp = True

# choose the pretokenizer, for splitting to words
pretokenizer_type = "whitespace"
thread_count = 24

# how many documents are batched for parallelism
batch_size = 5000

class WhitespaceDecoder:
    def decode(self, x: list[str]) -> str:
        return " ".join(x)

class Chunker:
    def __init__(self, pretokenizer_type="whitespace"):
        if pretokenizer_type == "whitespace":
            self.pretok = pre_tokenizers.WhitespaceSplit()
            #self.pretok = pre_tokenizers.Whitespace()
            self.decoder = WhitespaceDecoder()

        elif pretokenizer_type == "bytelevel":
            self.pretok = pre_tokenizers.ByteLevel()
            self.decoder = decoders.ByteLevel()

        else:
            raise ValueError("pretokenizer: {pretokenizer_type} is not supported")


        # we need to normalize or the pretokenizer gets unexpected characters
        #normalizer = normalizers.BertNormalizer(lowercase=False)
        self.normalizer = normalizers.NFC()

    def _norm(self, x: str) -> str:
        return self.normalizer.normalize_str(x)

    def _encode(self, x: str) -> list[str]:
        # pretok also returns positions, so we remove them
        return list(map(lambda x: x[0], self.pretok.pre_tokenize_str(x)))

    def _decode(self, x: list[str]) -> str:
        return self.decoder.decode(x)


    def chunk_document(self, document: tuple[int, str]) -> list[str]:
        try:
            parts = self._encode(self._norm(document))
        except Exception as e:
            print(e)
            exit(-1)

        try:
            chunks = list(map(self._decode, chunked(parts, chunksize)))
        except Exception as e:
            print(e)

        return chunks



def create_index(client: Elasticsearch, index_name: str):
    """Creates an index in Elasticsearch if one isn't already there."""
    client.indices.create(
        index=index_name,
        body={
            "settings": {},
            "mappings": {
                "properties": {
                    "text": {
                        "type": "text",
                        "similarity": "boolean",
                        "index_options": "positions",
                    },
                }
            },
        },
        #ignore=400,
    )
    '''
    "index": {
        "highlight": {
            #"max_analyzed_offset": 10000000
            }
    }'''

def search_phrase(entityA: str, entityB: str, num_results: int = 10, max_slop: int = 100):
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
    res = es.search(index=index_name, body=query)

    return res

def print_hits(res):
    print("Got %d Hits:" % res['hits']['total']['value'])
    for hit in res['hits']['hits']:
        print(f'{hit["_source"]}')


def action_loader(dataloader):
    # because we chain all files, we use
    for id, chunk in dataloader:
        assert isinstance(chunk, str), "chunk has to be a str"
        yield create_action(chunk, id, index_name)


def lm_reader(filename: str):
    rdr = Reader(filename)
    return rdr.stream_data()


def create_action(src: str, id: int, index_name: str):
    return { "_op_type": "create",
             "_index": index_name,
             "_id": id,
#            "_type": "document",
             "_source": {"text": src},
           }

if __name__ == '__main__':
    es = Elasticsearch()

    #es.indices.delete(index=index_name)#, ignore=[400, 404])
    if not es.indices.exists(index_name):
        create_index(es, index_name)
    #create_index(es, index_name)

    # chains all file readers
    dataiter = chain(*[lm_reader(fn) for fn in filelist])

    chunker = Chunker(pretokenizer_type)

    with Pool(processes=thread_count) as pool:
        if use_mp:
            chunklist = pool.imap_unordered(chunker.chunk_document, dataiter)
            processed = action_loader(enumerate(flatten(chunklist)))


        else:
            chunklist = map(chunker.chunk_document, dataiter)
            processed = action_loader(enumerate(flatten(chunklist)))


        if mode == "dryrun":
            for action in tqdm(processed):
                pass
        elif mode == "client":
            pass

        elif mode == "index":
            try:
                for ok, info in tqdm(streaming_bulk(es, processed), unit="chunk"):
                    if not ok:
                        raise Exception(info)
                        break
            except Exception as e:
                error = e
                print(e)
        else:
            raise ValueError("mode: {mode} is not supported")


