set -eux

echo "Packaging aggregated OSM data needed by backend into a zip file"

DATA_DIR=$1
OUTPUT_FILE=$2

# See
# https://stackoverflow.com/questions/23929235/multi-line-string-with-extra-space-preserved-indentation
LICENSE_NOTICE=`cat << END
The files in this zip-file are extracted from the full
OpenStreetMap data export that include all edit histories.

This data is (c) OpenStreetMap contributors and distributed
under the Open Database License (ODbL), see:

   https://www.openstreetmap.org/copyright

The download date and md5 checksum of the original .osm.pb
data export are included in this zip file. These can be used to
determine the exact data dump that was used.

For further details on how the data was extracted and
processed, please see the source repositories

  - https://github.com/tagdynamics-org/osm-extract-tags
  - https://github.com/tagdynamics-org/osm-tag-aggregator

matias.dahl@iki.fi
END
`

echo "$LICENSE_NOTICE" > $DATA_DIR/LICENSE.txt

zip $OUTPUT_FILE \
    $DATA_DIR/LICENSE.txt \
    $DATA_DIR/osm-input/download-date \
    $DATA_DIR/osm-input/history.osm.pbf.md5 \
    $DATA_DIR/aggregates/*
