package com.it21learning.ingestion.kafka;

import com.it21learning.ingestion.common.*;

public class UserConsumer extends IT21Consumer {
	//kafka topic
	@Override
	protected String getKafkaTopic() {
		return "users";
	}
	//the flag for how to commit the consumer reads
	@Override
	protected Boolean getKafkaAutoCommit() {
		return false;
	}
	//the max # of records polled
	@Override
	protected int getMaxPolledRecords() {
		return 2000;
	}
	//the max # of records polled
	@Override
	protected int getMaxPollIntervalMillis() {
		return 6000;
	}
	//consumer group
	@Override
	protected String getKafkaConsumerGrp() {
		return "grpUsers";
	}
	
	//constructor
	public UserConsumer() {
	}

	//writers
	@Override
	protected Persistable[] getWriters() {
		return new Persistable[] {
//			new MongoWriter("users", new com.it21learning.ingestion.data.mongo.UserParser()),
			new HBaseWriter("events_db:users", new com.it21learning.ingestion.data.hbase.UserParser())
		};
	}
}
