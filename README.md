# SentimentAnalysisBigData

Commands to Setup Environment
Create EMR Cluster using AWS Console.
 
PySpark - Comes with EMR Cluster.
MongoDB –
•	scp -i admin-key-pair.pem mongoex.tar hadoop@<Public DNS of Node>:/home/hadoop
•	scp -i admin-key-pair.pem mongodb-org-4.2.repo <Public DNS of Node>m:/home/hadoop
•	sudo cp	mongodb-org-4.2.repo /etc/yum.repos.d
•	tar	-xvf mongoex.tar
•	sudo yum install -y	mongodb-org-4.2.15 mongodb-org-server-4.2.15 mongodb-org-shell4.2.15 mongodb-org-mongos-4.2.15 mongodb-org-tools-4.2.15 
•	sudo service mongod start
•	mongo
•	use sentimentdb;

Kafka Setup
scp -i admin-key-pair.pem "<Local Path>/kafka_2.13-3.4.0.tgz" hadoop@<Public DNS of Node>:/home/hadoop
tar -xzf kafka_2.13-3.4.0.tgz
cd kafka_2.13-3.4.0
bin/zookeeper-server-start.sh config/zookeeper.properties &
bin/kafka-server-start.sh config/server.properties &
bin/kafka-topics.sh --create --replication-factor 1 --partitions 1 --bootstrap-server localhost:9092 --topic twitter

Pandas -  
sudo pip-3.6 install pandas

TextBlob-
sudo pip-3.6 install textblob

Python – Comes with EMR cluster.

Java and Spring Boot – 
Downloaded and configured environment variables for java. Spring boot library configured in pom.xml as below-
<groupId>org.springframework.boot</groupId>
<artifactId>spring-boot-starter-parent</artifactId>
<version>1.5.4.RELEASE</version>


Running Spark Job
spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 final_twitter_sentiment_analysis_v2.py

 
