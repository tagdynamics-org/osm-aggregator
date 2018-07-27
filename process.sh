set -eux

# run inside
#  sudo docker run -it --rm hseeberger/scala-sbt:8u171_2.12.6_1.1.6
#
# Before running, ensure the following are set:
#
#   export S3_INPUT_BUCKET_NAME=..
#   export S3_OUTPUT_BUCKET_NAME=..
#   export S3_ZIP_FILE=..
#

apt-get -y update && apt-get -y upgrade
apt-get -y install git zip mg tmux awscli s3cmd gradle

# get aggregator codes
cd /root
git clone --recurse-submodules https://github.com/tagdynamics-org/osm-tag-aggregator.git
cd osm-tag-aggregator
gradle wrapper

# download tag metadata
cd /root
mkdir in-data
cd in-data
aws s3 cp s3://$S3_INPUT_BUCKET_NAME/$S3_ZIP_FILE .

# unzip data into workdir
cd /root
mkdir work-dir
cd work-dir
unzip ../in-data/$S3_ZIP_FILE

# compute all aggregates
cd /root
mkdir work-dir/aggregates
cd osm-tag-aggregator
bash compute-all.sh /root/work-dir/tag-metadata/tag-history.jsonl /root/work-dir/aggregates/

# make output zip file
cd /root/workdir
rm ./tag-metadata/tag-history.jsonl
zip -r /root/$S3_FILE .