
#!/bin/bash

python pyacq/filter.py
python pyacq/clean.py
python pyacq/sample.py
python pyacq/final.py

# create init KG
cat $(find data/splits/split_1/ -type f | grep wikidata) | grep -P '^<http://www\.wikidata\.org/entity/' | sed 's@http://dbpedia.org/ontology/@http://mykg.org/wikidata/ontology/@g' > data/final/seed.nt

# create wikidata eval file
cat $(find data/splits/ -type f | grep wikidata) > data/final/eval.nt

# create text source
ABS_FILE_LIST=$(mktemp)
find data/final/split_1/ -type f | grep abstracts/film > $ABS_FILE_LIST
find data/final/split_2/ -type f | grep abstracts/film >> $ABS_FILE_LIST
find data/final/split_3/ -type f | grep abstracts/film >> $ABS_FILE_LIST

mkdir -p data/final/text
cat $ABS_FILE_LIST | while read -r file; do
    cp $file data/final/text/$(basename $file)
done

rm $ABS_FILE_LIST

# create json source
DBP_FILE_LIST=$(mktemp)
find data/final/split_1/ -type f | grep dbp/ > $DBP_FILE_LIST
find data/final/split_2/ -type f | grep dbp/ >> $DBP_FILE_LIST
find data/final/split_4/ -type f | grep dbp/ >> $DBP_FILE_LIST

echo $DBP_FILE_LIST
DBP_FILE="data/final/tmp-json.nt"
rm $DBP_FILE
cat $DBP_FILE_LIST | while read -r file; do
    cat "$file" >> $DBP_FILE
done

# rm $DBP_FILE_LIST

# create rdf source
DBO_FILE_LIST=$(mktemp)
find data/final/split_1/ -type f | grep dbp/ > $DBO_FILE_LIST
find data/final/split_2/ -type f | grep dbp/ >> $DBO_FILE_LIST
find data/final/split_5/ -type f | grep dbp/ >> $DBO_FILE_LIST

DBO_FILE="data/final/source.nt"
rm $DBO_FILE
cat $DBO_FILE_LIST | while read -r file; do
    cat "$file" >> $DBO_FILE
done

rm $DBO_FILE_LIST

python pyacq/tojson.py