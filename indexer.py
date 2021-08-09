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
        splits = x.split(" ")
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
    def __init__(self, pretokenizer_type="whitespace", sus_document_behaviour="split_to_chunks", chunksize=256):
        self.document_pos = 0
        self.chunk_pos = 0

        if pretokenizer_type == "whitespace":
            #self.pretok = pre_tokenizers.WhitespaceSplit()
            #self.pretok = pre_tokenizers.Whitespace()
            self.pretok = WhitespaceSplitter(sus_document_behaviour)
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
            es_helper,
            index_name,
            tmpdir,
            logger,
            id_,
            paralellism=-1,
            run_compressed=True):

        self.errorlimit = 1000
        self.errors = 0

        self.index_name = index_name
        self.es = es_helper

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

        self.bulk_request_size = 5000


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
            reader = Reader(self.filepath)
            for doc in reader.stream_data():
                self.check_health()
                self.document_pos += 1
                yield doc
        else:
            self.logger.info(f"running on decompressed data")
            decompressed_filename = self._strip_fileext(filename, ".zst")
            decompressed_path = os.path.join(self.tmpdir, decompressed_filename)

            if not os.path.isfile(decompressed_path):
                self.logger.info(f"decompressing: {self.filepath}")
                subprocess.run(["zstd", "-d", self.filepath, "-o", decompressed_path])
            else:
                self.logger.info(f"found and use decompressed file: {decompressed_path}")

            with jsonlines.open(decompressed_path) as reader:
                for doc in reader.iter():
                    self.check_health()
                    yield doc

    def _action_loader(self, chunks):
        for pos, chunk in enumerate(chunks):
            if not isinstance(chunk, str):
                self.errors += 1
                raise ValueError(f"chunk has to be a string but was: {type(chunk)}")
            # assume there are maximum 100 Fileloaders/ dataset shards
            yield self._create_action(chunk, pos * 100 + self.id_, self.index_name)


    def ingest(self, es_helper):
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
                for ok, info in tqdm(es_helper.streaming_bulk(processed), unit="chunk"):
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

        self.logger.info(f"finished loading file {self.filepath}, id: {self.id_} with error count: {self.errors}, document count: {self.document_pos} and chunk count: {self.chunk_pos}")


def truncate_error(e):
    return str(e)[:1000]


class ElasticLogFilter(logging.Filter):
    def filter(self, record):
        return not record.name.startswith("elasticsearch")


class ElasticHelper:
    def __init__(
            self,
            hosts,
            index_name,
            logger,
            bulk_request_size=1000,
            overwrite_index=False,
            timeout=30,
            ):

        self.hosts = hosts
        self.es = Elasticsearch(self.hosts, timeout=timeout)
        self.index_name = index_name
        self.overwrite_index = overwrite_index
        self.logger = logger
        self.bulk_request_size = bulk_request_size

        if self.overwrite_index:
            self.logger.warning(f"overwriting index is set to True")

    def streaming_bulk(self, processed):
        if self.overwrite_index:
            self.logger.warning(f"overwriting index is set to True")
            self.logger.warning(f"overwriting old index: {index_name}")
            self.es.indices.delete(index=index_name, ignore=[400, 404])

        if not self.es.indices.exists(index=index_name):
            self.logger.info(f"creating new index: {index_name}")
            self._create_index()

        return streaming_bulk(self.es, processed, chunk_size=self.bulk_request_size)

    def refresh(self):
        self.es.indices.refresh()

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

if __name__ == '__main__':
    logfile = f"logs/indexer_{datetime.utcnow().timestamp()}.log"
    if os.path.isfile(logfile):
        raise RuntimeError("logfile already exists")


    # configurables

    dryrun = False
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

    es_helper = None
    if not dryrun:
        es_helper = ElasticHelper(hosts, index_name, logger, overwrite_index=overwrite_index)


    for id_, filepath in enumerate(filelist):
        chunker = Chunker(pretokenizer_type, chunksize)
        ingester = Ingester(filepath, chunker, dryrun, es_helper, index_name, tmpdir, logger, id_)
        ingester.ingest(es_helper)
        es_helper.refresh()
