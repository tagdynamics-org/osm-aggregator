set -eux

# input JSONL file with exported tag data
INPUT_DATE=$1

# where to place aggregated data. Directory should already exist
OUT_DIR=$2

bash launch.sh LIVE_REVCOUNTS        $INPUT_DATA $OUT_DIR/live-revcounts.jsonl
bash launch.sh TOTAL_REVCOUNTS       $INPUT_DATA $OUT_DIR/total-revcounts.jsonl
bash launch.sh TRANSITION_COUNTS     $INPUT_DATA $OUT_DIR/transition-counts.jsonl
bash launch.sh PER_DAY_DELTA_COUNTS  $INPUT_DATA $OUT_DIR/per-day-delta-counts.jsonl

echo "compute-all: Done"