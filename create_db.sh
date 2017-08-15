#!/bin/bash

# assumes cypher-shell is in path
# OSX: "/Applications/Neo4j Community Edition 3.2.3.app/Contents/Resources/app/bin"

# must have write access to database directory

# must set NEO4J username and password


DATA_DIR=`pwd`/data
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=passwd

# from http://opencitations.net/download
AR_NUM=5255386
BE_NUM=5255323
BR_NUM=5255365
ID_NUM=5255368
RA_NUM=5255359
RE_NUM=5255395

if [ `uname -s` == Linux ]; then
    DB_DIR="/var/lib/neo4j/data"
    if [ ! -w ${DB_DIR} ]; then
        echo "Database directory not writeable: ${DB_DIR}"
        echo "sudo and try again?"
        exit -1
    fi
fi

if [ `uname -s` == Darwin ]; then
    DB_DIR=`pwd`/db
    echo "Deleting database directory: " $DB_DIR
    read -p "Stop database and press ENTER to continue:"

    rm -rf $DB_DIR
    mkdir $DB_DIR

    read -p "Start database and press ENTER to contine:"
    mkdir $DB_DIR/import
    open http://127.0.0.1:7474/browser/

    read -p "Set database credentials and press ENTER to continue:"
fi

date
echo "Downloading and extracting files..."
rm -rf data
which dar
if [ $? == 1 ]; then
    echo "Need to install 'dar'...";
    sudo apt-get install dar
fi

download_and_extract ()
{
    DIR=$1
    ID=$2
    URL=https://ndownloader.figshare.com/articles/${ID}/versions/1
    echo "Creating $DIR from $URL..."

    # download file
    wget -O ${ID}.zip ${URL}

    # get corpus .zip
    unzip ${ID}.zip \*.zip
    rm ${ID}.zip

    # get .dar files
    unzip *.zip \*.dar
    rm *corpus*.zip

    # extract .dar files
    DAR_GROUP=`ls *.dar | head -n 1 | cut -d. -f1`
    OUTPUT_PATH=data/${DIR}
    mkdir -p ${OUTPUT_PATH}
    cwd=`pwd`
    cd "${OUTPUT_PATH}"
    dar -O -x ${cwd}/${DAR_GROUP}
    cd ${cwd}
    rm *.dar
}

download_and_extract "ar" $AR_NUM
download_and_extract "be" $BE_NUM
download_and_extract "br" $BR_NUM
download_and_extract "id" $ID_NUM
download_and_extract "ra" $RA_NUM
download_and_extract "re" $RE_NUM
echo "download and extract done."
date

echo -n "Creating CSV files... "
node --max-old-space-size=4096 bin/occ-to-neo4j.js $DB_DIR $DATA_DIR
echo "done."
date

echo "Loading CSV files into database..."
cat create_db.cyp | cypher-shell
echo "all done."
date