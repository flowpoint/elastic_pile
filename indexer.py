#!/bin/python

from lm_dataformat import Reader
from tqdm import tqdm
from itertools import accumulate, chain, starmap
from functools import reduce
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, streaming_bulk, parallel_bulk
from multiprocessing import pool, Pool, Queue
import json
import os

from more_itertools import chunked

from tokenizers import pre_tokenizers
from tokenizers import decoders
from tokenizers import normalizers


#mode = "dryrun"
#mode = "client"
mode = "index"

# the order of files is important, because we enumerate the id across files
filelist = sorted(["pile/val.jsonl.zst"])
#filelist = sorted(["../00.jsonl.zst"])

for f in filelist:
    if not os.path.isfile(f):
        raise Exception("the file: {f} doesn't exist")


index_name = "test_index"

# size in words according to pre_tokenizer
chunksize = 256
use_mp = True

# choose the pretokenizer, for splitting to words
pretokenizer_type = "whitespace"
thread_count = 24

# how many documents are batched for parallelism
batch_size = 5000

if pretokenizer_type == "whitespace":
    #pretok = pre_tokenizers.WhitespaceSplit()
    pretok = pre_tokenizers.Whitespace()
    decode_fn = lambda x: " ".join(x)

    pretok = pre_tokenizers.ByteLevel()
    decoder = decoders.ByteLevel()
    decode_fn = decoder.decode


# we need to normalize or the pretokenizer gets unexpected characters
#normalizer = normalizers.BertNormalizer(lowercase=False)
normalizer = normalizers.NFC()


def chunk_document(numbered_document)-> [[str]]:
    filelocal_document_number, document = numbered_document

    try:
        parts = map(lambda x: x[0], pretok.pre_tokenize_str(normalizer.normalize_str(document)))
        chunks = list(map(decode_fn, chunked(parts, chunksize)))
    except Exception as e:
        print(e)

    return chunks


def fileloader(filename: str) -> [str]:
    rdr = Reader(filename)

    batch = []
    with Pool(processes=thread_count) as pool:
        for filelocal_document_number, document in enumerate(rdr.stream_data()):
            # track document number/order through parallel execution
            batch.append((filelocal_document_number, document))

            if filelocal_document_number % batch_size == 0:
                if use_mp:
                    chunk_batch = pool.map(chunk_document, batch)
                else:
                    chunk_batch = map(chunk_document, batch)

                batch = []

                for chunks in chunk_batch:
                    for chunk in chunks:
                        assert isinstance(chunk, str), "chunk in fileloader has to be a str"
                        yield chunk


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

def search_phrase(entityA: str, entityB: str, num_results=10, max_slop=100):
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



def create_action(src: str, id: int, index_name: str):
    return { "_op_type": "create",
           "_index": index_name,
           "_id": id,
#           "_type": "document",
           "_source": {"text": src},
           }

def action_loader(dataloader):
    # because we chain all files, we use
    for id, chunk in enumerate(dataloader):
        assert isinstance(chunk, str), "chunk has to be a str"
        yield create_action(chunk, id, index_name)


es = Elasticsearch()

#es.indices.delete(index=index_name)#, ignore=[400, 404])
create_index(es, index_name)

# chain iterators for all files
dataloader = chain(*[fileloader(fn) for fn in filelist])



if mode == "dryrun":
    for chunk in tqdm(dataloader, unit="chunk"):
        pass

elif mode == "client":
    pass

elif mode == "index":
    # save error to var for interactive debug
    error = None
    try:
        for ok, info in tqdm(streaming_bulk(es, action_loader(dataloader)), unit="chunk"):
            if not ok:
                raise Exception(info)
                break
    except Exception as e:
        error = e
        print(e)
