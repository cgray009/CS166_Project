cp ./data/*.csv /tmp/$USER/myDB/data/

psql -h localhost -p $PGPORT $USER"_DB" < ./sql/create.sql
