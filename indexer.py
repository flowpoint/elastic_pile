#!/bin/python
import os
import sys
import json
import logging
from time import time
from datetime import datetime
from itertools import accumulate, chain, starmap, cycle
from functools import reduce
from multiprocessing import pool, Pool, Queue
from typing import List, Tuple, Dict, Generator, Iterator, Sequence
import subprocess

from more_itertools import chunked, flatten, sliced
from tqdm import tqdm # type: ignore

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, streaming_bulk, parallel_bulk
from tokenizers import pre_tokenizers, decoders, normalizers # type: ignore

import jsonlines # type: ignore
from lm_dataformat import Reader # type: ignore


class WhitespaceSplitter:
    def __init__(self, 
            sus_document_behaviour="split_to_chunks",
            expected_word_len=8, 
            sus_len_factor=8, 
            chunksize=256,
            ):

        self.sus_document_behaviour = sus_document_behaviour
        self.expected_word_len = expected_word_len
        self.sus_word_len = sus_len_factor * self.expected_word_len
        self.chunksize = chunksize


    def pre_tokenize_str(self, x: str) -> tuple[Sequence[tuple[str, int]], bool]:
        sus = False
        splits = x.split()
        # if the splits are suspiciously small, it wasn't split well, 
        # 16 characters per word is suspicious, 8 char is avg for english
        # use character based chunking instead of word based
        if len(splits) * self.sus_word_len < len(x):
            sus = True
            if self.sus_document_behaviour == "split_to_words":
                words = list(sliced(x, self.expected_word_len))
            elif self.sus_document_behaviour == "drop":
                words = [""]
            else:
                words = list(sliced(x, self.chunksize))
        else:
            words = splits

        # return -1 as word positions for this pre_tokenizer
        # and if document is sus
        return words, sus


class WhitespaceDecoder:
    def decode(self, x: list[str]) -> str:
        return " ".join(x)

class Chunker:
    def __init__(self, pretokenizer_type="whitespace", chunksize=256):
        self.document_pos = 0
        self.chunk_pos = 0

        if pretokenizer_type == "whitespace":
            #self.pretok = pre_tokenizers.WhitespaceSplit()
            #self.pretok = pre_tokenizers.Whitespace()
            self.pretok = WhitespaceSplitter()
            self.decoder = WhitespaceDecoder()

        elif pretokenizer_type == "bytelevel":
            self.pretok = pre_tokenizers.ByteLevel()
            self.decoder = decoders.ByteLevel()

        else:
            raise ValueError("pretokenizer: {pretokenizer_type} is not supported")

        # we need to normalize or the pretokenizer gets unexpected characters
        #normalizer = normalizers.BertNormalizer(lowercase=False)
        self.normalizer = normalizers.NFC()
        self.chunksize = chunksize

    def _norm(self, x: str) -> str:
        return self.normalizer.normalize_str(x)

    def _encode(self, x: str) -> list[str]:
        # pretok also returns positions, so we remove them
        words, sus = self.pretok.pre_tokenize_str(x)
        if sus:
            print(words)
        return words, sus

    def _decode(self, x: list[str]) -> str:
        return self.decoder.decode(x)

    def chunk_document(self, numbered_document: str) -> list[str]:
        document = numbered_document
        if not isinstance(document, str):
            raise ValueError(f"document has to be str but was: {type(document)}")
        else:
            parts, sus = self._encode(self._norm(document))

        # important that some of these maps are evaluated or they spam your memory
        chunks = list(map(self._decode, chunked(parts, self.chunksize)))
        self.document_pos += 1
        self.chunk_pos += len(parts)
        if sus:
            logger.warning(f"document {self.document_pos}, equal to chunk: {self.chunk_pos} was badly chunked, alternative chunking was used")

        return chunks


class Ingester:
    def __init__(self,
            filepath,
            chunker,
            dryrun,
            tmpdir,
            logger,
            id_,
            paralellism=-1,
            run_compressed=True):

        self.errorlimit = 1000
        self.errors = 0

        self.filepath = filepath
        _, filename = os.path.split(self.filepath)
        self.run_compressed = run_compressed

        self.document_pos = 0
        self.chunk_pos = 0

        self.chunker = chunker

        self.logger = logger
        self.paralellism = paralellism
        self.tmpdir = tmpdir
        self.dryrun = dryrun

        self.id_ = id_


    def _strip_fileext(self, filepath, ext):
        return filepath[::-1].replace(ext[::-1], "", 1)[::-1]

    def _create_action(self, src: str, id_: int, index_name: str):
        return { "_op_type": "create",
                 "_index": index_name,
                 "_id": id_,
                 #"_source": {"text": src},
                 "text": src,
               }

    def check_health(self):
        if self.errorlimit < self.errors:
            self.logger.error(f"too many errors: {self.errors}, quitting")
            sys.exit(1)

    def _reader(self):
        if self.run_compressed:
            self.logger.info(f"running on compressed data")
            reader = Reader(filepath)
            for doc in reader.stream_data():
                self.check_health()
                yield doc
        else:
            self.logger.info(f"running on decompressed data")
            decompressed_filename = self._strip_fileext(filename, ".zst")
            decompressed_path = os.path.join(self.tmpdir, decompressed_filename)

            if not os.path.isfile(decompressed_path):
                self.logger.info(f"decompressing: {filepath}")
                subprocess.run(["zstd", "-d", filepath, "-o", decompressed_path])
            else:
                self.logger.info(f"found and use decompressed file: {decompressed_path}")

            with jsonlines.open(decompressed_path) as reader:
                for doc in reader.iter():
                    self.check_health()
                    yield doc

    def _action_loader(self, chunks):
        for pos, chunk in enumerate(chunks):
            if not isinstance(chunk, str):
                self.errors+=1
                raise ValueError(f"chunk has to be a string but was: {type(chunk)}")
            # assume there are maximum 100 Fileloaders/ dataset shards
            yield self._create_action(chunk, pos * 100 + self.id_, index_name)


    def ingest(self):
        starttime = time()
        lastlog = starttime

        self.logger.info(f"started Fileloader load_file {self.filepath}")

        reader = self._reader()

        with Pool(processes=max(self.paralellism, 1)) as pool:
            if self.paralellism > 0:
                chunklist = pool.imap_unordered(self.chunker.chunk_document, reader)
                processed = self._action_loader(flatten(chunklist))

            elif self.paralellism == -1:
                chunklist = map(self.chunker.chunk_document, reader)
                processed = self._action_loader(flatten(chunklist))
            else:
                self.errors += 1
                raise ValueError(f"invalid paralellism: {paralellism}")

            if self.dryrun:
                for action in tqdm(processed):
                    self.chunk_pos += 1
                    pass
                return

            try:
                for ok, info in tqdm(streaming_bulk(es, processed, chunk_size=bulk_request_size), unit="chunk"):
                    self.chunk_pos += 1
                    try:
                        if time() - lastlog > 60:
                            self.logger.info(f"processed {self.chunk_pos} chunks")
                            lastlog = time()

                        if not ok:
                            self.errors += 1
                            raise Exception(info)

                    except Exception as e:
                        self.errors += 1
                        self.logger.error(f"error in streaming_bulk:\n{truncate_error(e)}")
            except Exception as e:
                self.errors+=1
                self.logger.error(f"error in streaming_bulk:\n{truncate_error(e)}")

        self.logger.info(f"finished loading file {filepath}")


class ElasticLogFilter(logging.Filter):
    def filter(self, record):
        return not record.name.startswith("elasticsearch")

def create_index(client: Elasticsearch, index_name: str):
    """Creates an index in Elasticsearch if one isn't already there."""
    client.indices.create(
        index=index_name,
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
                        "norms": False,
                        "similarity": "BM25",
                        "index_options": "positions",
                    },
                }
            },

        },
        #ignore=400,
    )

def search_phrase(entityA: str, entityB: str, num_results: int = 10, max_slop: int = 20):
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

def truncate_error(e):
    return str(e)[:1000]


if __name__ == '__main__':
    logfile = f"logs/indexer_{datetime.utcnow().timestamp()}.log"
    if os.path.isfile(logfile):
        raise RuntimeError("logfile already exists")


    # configurables

    dryrun = True
    #mode = "client"
    #mode = "index"

    bulk_request_size = 5000

    hosts = ["localhost"]
    tmpdir = "tmp"

    overwrite_index = False
    run_compressed = False
    break_on_error = True

    # the order of files is important, because we enumerate the id across files
    #filelist = sorted(["pile/val.jsonl.zst"])
    filelist = sorted(["../00.jsonl.zst"])
    for f in filelist:
        if not os.path.isfile(f):
            raise FileNotFoundError(f)

    index_name = "test_index"

    # size in words according to pre_tokenizer
    chunksize = 256
    # -1 for singlecore n >= 1 for using n parallelism
    paralellism = -1
    assert paralellism == -1 or paralellism > 0

    # choose the pretokenizer, for splitting to words
    pretokenizer_type = "whitespace"
    skip_chunks = -1


    elasticlogger = logging.getLogger("elasticsearch")
    elasticlogger.addFilter(ElasticLogFilter())

    logging.basicConfig(filename=logfile, level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    logger.addFilter(ElasticLogFilter())

    if not dryrun:
        es = Elasticsearch(hosts, timeout=30)

    if not dryrun:
        if overwrite_index:
            logger.info(f"overwriting old index: {index_name}")
            es.indices.delete(index=index_name, ignore=[400, 404])

        if not es.indices.exists(index=index_name):
            logger.info(f"creating new index: {index_name}")
            create_index(es, index_name)

    for id_, filepath in enumerate(filelist):
        chunker = Chunker(pretokenizer_type, chunksize)
        ingester = Ingester(filepath, chunker, dryrun, tmpdir, logger, id_)
        ingester.ingest()

        #logger.info(f"processed {i} documents")
