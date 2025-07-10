#!/bin/bash

OUT="wikidata-films"
mkdir -p $OUT

crawl_uri_list() {
        LIST=$1
        cat $LIST | while read -r uri; do
                suffix=${uri##*/}
                echo $suffix $uri 1<&2
                rapper -g $uri > $OUT/$suffix.nt
                sleep 0.1
        done
}

crawl_uri_list $1
