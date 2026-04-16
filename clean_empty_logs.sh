#!/bin/bash

# recupero il path
SCRIPT_DIR=$(dirname "$(realpath "$0")")

# cancello
#find "$SCRIPT_DIR" -name "*.log" -empty -delete
# non va bene perchè cancellerebbe anche i file aperti 


find "$SCRIPT_DIR" -name "*.log" -empty | while read f; do
    if ! lsof "$f" > /dev/null 2>&1; then
        echo "$f"  # sostituisci con -delete quando sei sicuro
    fi
done


#-delete