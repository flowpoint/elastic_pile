# elastic_pile
scripts for handeling the pile with elasticsearch and augmenting it with knowledge bases

currently this is just a hacked together way to load the pile dataset into elasticsearch.

on a 12 core and nvme ssd, the ~450 MB val.json file took 3.5 minutes.
the index was around 1.3 gb for the file.
extrapolated to the pile (naively extrapolated 30 files at 15GB each => 30 * 30 * 3.5 => 53h)

### to push pile data into elasticsearch:

edit the indexer script to suit your needs

pipenv is recommended:
pip install --user pipenv

enter pipenv shell:
pipenv shell
pipenv install

run ./start to start the elastic docker container

run python indexer.py


todo:
- logging
- tests

optional:
- benchmarks
- investigate asynchrony or restructure code for maybe more performance

then add knowledge base:
either use a db (like BlazeGraph)
or run naively through rdf-files
