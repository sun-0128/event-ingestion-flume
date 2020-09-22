package com.it21learning.ingestion.kafka;

import com.it21learning.ingestion.common.*;

public class TrainConsumer extends IT21Consumer {
	//kafka topic
	@Override
	protected String getKafkaTopic() {
		return "train";
	}
	//the flag for how to commit the consumer reads
	@Override
	protected Boolean getKafkaAutoCommit() {
		return false;
	}
	//the max # of records polled
	protected int getMaxPolledRecords() {
		return 1000;
	}
	//the max # of records polled
	protected int getMaxPollIntervalMillis() {
		return 3600;
	}
	//consumer group
	@Override
	protected String getKafkaConsumerGrp() {
		return "grpTrain";
	}
	
	//constructor
	public TrainConsumer() {
	}

	//writers
	@Override
	protected Persistable[] getWriters() {
		return new Persistable[] {
//			new MongoWriter("train", new com.it21learning.ingestion.data.mongo.TrainParser()),
			new HBaseWriter("events_db:train", new com.it21learning.ingestion.data.hbase.TrainParser())
		};
	}
}
