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

import tracemalloc
from memory_profiler import profile

import cProfile, pstats, io
from pstats import SortKey
from time import sleep

#tracemalloc()

tracemalloc.start()
es = Elasticsearch()

snaps = []
while 1:
    s = tracemalloc.take_snapshot()
    snaps.append(s)
    sleep(1)

