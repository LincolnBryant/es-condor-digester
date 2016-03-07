#!/bin/bash
if [ $# -ne 2 ]; then
  # Need at least two arguments, otherwise something has gone awry.
  echo "Usage: condor_elasticsearch.sh <history file> <queue>"
  exit 1 
fi

# inotify will alert us to all events. when the history file is about to
# to be rotated, it'll be closed. We skip it because want to read the file 
# that comes after it.
if [ $1 == 'history' ]; then
  echo "Skipping live history file"
  exit 0
fi

# Run the indexer
cd /var/lib/condor/spool.$2/history
python /usr/local/bin/condor_history_indexer.py $1 >> /var/log/condor_elasticsearch.log.$2

# compress it and stash it (ha)
cp $1 /stash2/sys/condor_history/$(hostname)/$2/
gzip /stash2/sys/condor_history/$(hostname)/$2/$1
