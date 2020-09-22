package com.it21learning.ingestion.kafka;

import com.it21learning.ingestion.common.*;

public class UserFriendsConsumer extends IT21Consumer {
	//kafka topic
	@Override
	protected String getKafkaTopic() {
		return "user_friends";
	}
	//the flag for how to commit the consumer reads
	@Override
	protected Boolean getKafkaAutoCommit() {
		return true;
	}
	//the max # of records polled
	@Override
	protected int getMaxPolledRecords() {
		return 9000;
	}
	//the max # of records polled
	@Override
	protected int getMaxPollIntervalMillis() {
		return 15000;
	}
	//consumer group
	@Override
	protected String getKafkaConsumerGrp() {
		return "grpUserFriends";
	}

	//constructor
	public UserFriendsConsumer() {
	}

	//writers
	@Override
	protected Persistable[] getWriters() {
		return new Persistable[] {
//			new MongoWriter("user_friend", new com.it21learning.ingestion.data.mongo.UserFriendsParser()),
			new HBaseWriter("events_db:user_friend", new com.it21learning.ingestion.data.hbase.UserFriendsParser())
		};
	}
}
