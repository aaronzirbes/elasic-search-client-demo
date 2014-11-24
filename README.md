ElasticSearch POC
=================

Running ElasticSearch from Docker
---------------------------------

    # Pull down the latest elasticsearch Docker image
    docker pull dockerfile/elasticsearch

    # Run elastic search as a daemon
    docker run -d -p 9200:9200 -p 9300:9300 dockerfile/elasticsearch

Running ElasticSearch from homebrew
-----------------------------------

    # Install elasticsearch
    brew install elasticsearch

    # Run elasticsearch
    elasticsearch

Running the code
----------------

    ./gradle run


