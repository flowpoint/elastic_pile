#!/bin/python
import os
import json
import logging
from time import time
from datetime import datetime
from itertools import accumulate, chain, starmap, cycle
from functools import reduce
from multiprocessing import pool, Pool, Queue
from typing import List, Tuple, Dict, Generator, Iterator
import subprocess

from more_itertools import chunked, flatten
from tqdm import tqdm # type: ignore

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, streaming_bulk, parallel_bulk
from tokenizers import pre_tokenizers, decoders, normalizers # type: ignore
import jsonlines
from lm_dataformat import Reader # type: ignore
from time import sleep, time


def create_action(src: str, total_pos: int, index_name: str):
    return { "_op_type": "create",
             "_index": index_name,
             "_id": total_pos,
#            "_type": "text",
             #"_source": {"text": src},
             "text": src,
           }


es = Elasticsearch(["localhost"])

gen = [
        create_action("hel", 1000000001, "test_index")
        ]

for ok, info in streaming_bulk(es, gen):
    print(ok)
    print(info)
    pass




while 1:
    print(time())
    sleep(30)
