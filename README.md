[![Build Status](https://travis-ci.com/tagdynamics-org/osm-tag-aggregator.svg?branch=master)](https://travis-ci.com/tagdynamics-org/osm-tag-aggregator)

# osm-tag-aggregator

[osm-extract-tags](https://github.com/tagdynamics-org/osm-extract-tags) is a tool
for extracting tag metadata from an OpenStreetMap (OSM) data export with the entire
history of the OSM. This repo (osm-tag-aggregator) contains a tool for computing
various aggregates from this data. The below aggregates are currently implemented:

#### LIVE_REVCOUNTS

For each tag state, compute the total number of map elements that are currently in 
that state. Only include tag states with >= 5 map elements.

Example output:

```
{"key":{"state":"DEL","tags":[]},"n":543}
{"key":{"state":"VIS","tags":[]},"n":266}
{"key":{"state":"VIS","tags":["2:pizzeria"]},"n":128}
{"key":{"state":"VIS","tags":["2:bench"]},"n":125}
...
```

#### TOTAL_REVCOUNTS
For each tag state, compute the total number of map elements that at some point have 
been in that state.

Example output:

```
{"key":{"state":"DEL","tags":[]},"n":2060}
{"key":{"state":"VIS","tags":[]},"n":939}
{"key":{"state":"VIS","tags":["2:water"]},"n":455}
{"key":{"state":"VIS","tags":["1:wall"]},"n":430}
...
```

#### TRANSITION_COUNTS

For each transition between tag states, compute the total number of times a map element
has undergone that transition. If the same map element has transitioned multiple times
all transitions are counted. Skip transitions that have ocurred <5 times.

Example output:

```
...
```

#### PER_DAY_DELTA_COUNTS

For each day and tag state, compute a delta (a positive or negative number, but not 0) giving
how the number of map elements with that tag state has changed during that day. By integrating the delta:s over time one can get the number of map elements in a tag state for any given day (during OSM's recorded history).

Example output:

```
...
```

##### Terminology

 - A **map element** means either a node, way or a relation in the OSM data.
 - **element states**
    - Visible with tags `["0:tag_value1", "a:tag_value2"]`. The codes `0` and `a` refer to the [list of tags](https://github.com/tagdynamics-org/osm-extract-tags#extract-tags-from-an-osm-file) selected or extraction by osm-extract-tags.
    - Deleted

The implementation is streaming and does not need to load the entire input file into memory.


------

## Cloning

This repo contains a git submodule with [test data](https://github.com/tagdynamics-org/testdata).
To fetch this there are two options when cloning the repo:

```bash
# option 1
git clone https://github.com/tagdynamics-org/osm-tag-aggregator.git
git submodule update --init

# option 2
git clone --recurse-submodules https://github.com/tagdynamics-org/osm-tag-aggregator.git
```

## Running unit tests

Unit tests can be run from command line as follows (provided gradle is first installed):

```bash
gradle wrapper
./gradlew test
```

### IntelliJ IDEA setup up

Import as a gradle project. During importing, the IDE may ask for the "gradle home directory". See [here](https://stackoverflow.com/questions/18495474/how-to-define-gradles-home-in-idea) for instructions on how to determine this.

### Syntax for running

```bash
bash launch.sh aggregator /path/to/input.jsonl /path/to/output.jsonl
```

 - `aggregator` is one of `LATEST_REVCOUNTS`, ..., `PER_DAY_DELTA_COUNTS`. See above.
 - input file is output from the [osm-extract-tags](https://github.com/tagdynamics-org/osm-extract-tags) tool.

### Running in the cloud

```
export DEBIAN_FRONTEND=noninteractive
sudo apt-get -y update
sudo apt-get -y upgrade
sudo apt-get -y install git zip mg tmux gradle

# install gradle wrapper & install dependencies
cd osm-tag-aggregator && gradle wrapper test && cd ..

# install docker: https://docs.docker.com/install/linux/docker-ce/ubuntu/

# https://hub.docker.com/r/hseeberger/scala-sbt/
sudo docker pull hseeberger/scala-sbt:8u171_2.12.6_1.1.6
sudo docker run -v `pwd`/data/:/data -v `pwd`/osm-tag-aggregator:/code -it --rm hseeberger/scala-sbt:8u171_2.12.6_1.1.6
cd /code
gradle wrapper
./gradlew test

mkdir /data/aggregates
bash launch.sh LIVE_REVCOUNTS /data/tag-metadata/tag-history.jsonl /data/aggregates/latest-revs.jsonl
bash launch.sh TOTAL_REVCOUNTS /data/tag-metadata/tag-history.jsonl /data/aggregates/total-rev-counts.jsonl
bash launch.sh PER_DAY_DELTA_COUNTS /data/tag-metadata/tag-history.jsonl /data/aggregates/per-day-delta-counts.jsonl
bash launch.sh TRANSITION_COUNTS /data/tag-metadata/tag-history.jsonl /data/aggregates/transition-counts.jsonl
```

```
 - OSM input data ~5000M map elements + their version histories
 - Exported revision history JSONL file: 552M lines (5528 batches @100k)

(m5.xlarge; 16G memory; 4VCPU)
LATEST_REVCOUNTS      25m
TRANSITION_COUNTS     27m
TOTAL_REVCOUNTS       28m
PER_DAY_DELTA_COUNTS  35m

(t2.medium; 4G memory; 2VCPU)
LATEST_REVCOUNTS      1h33m
TRANSITION_COUNTS     ??
TOTAL_REVCOUNTS       ??
PER_DAY_DELTA_COUNTS  3h5m

```

 - [How to transfer data between EC2 instances](http://blog.e-zest.com/how-to-do-scp-from-one-ec2-instance-to-another-ec2-instance/)

## Contributions

Ideas, questions and/or contributions are welcome.

## License

Copyright 2018 Matias Dahl. Released under the [MIT license](LICENSE.md).

Please note that `osm-tag-extract` and `osm-tag-aggregator` are designed to process OpenStreetMap data. This data is available under the [Open Database License](https://openstreetmap.org/copyright). See also the [OSMF wiki](https://wiki.openstreetmap.org/wiki/GDPR) regarding OpenStreetMap
data and the [GDPR](https://gdpr-info.eu/).

