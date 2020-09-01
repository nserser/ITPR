mvn clean compile assembly:single
mv target/RDR-0.0.1-SNAPSHOT-jar-with-dependencies.jar target/rdr.jar
scp -P 23 target/rdr.jar abkakria@10.35.114.253:/home/abkakria/
