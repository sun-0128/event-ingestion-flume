package com.it21learning.ingestion.kafka;

import com.it21learning.ingestion.common.*;

public class EventAttendeesConsumer extends IT21Consumer {
	//kafka topic
	@Override
	protected String getKafkaTopic() {
		return "event_attendees";
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
		return 12000;
	}
	//consumer group
	@Override
	protected String getKafkaConsumerGrp() {
		return "grpEventAttendees";
	}
	
	//constructor
	public EventAttendeesConsumer() {
	}

	//writers
	@Override
	protected Persistable[] getWriters() {
		return new Persistable[] {
//			new MongoWriter("event_attendee", new com.it21learning.ingestion.data.mongo.EventAttendeesParser()),
			new HBaseWriter("events_db:event_attendee", new com.it21learning.ingestion.data.hbase.EventAttendeesParser())
		};
	}
}
