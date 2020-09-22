package com.it21learning.ingestion.data.mongo;

import com.mongodb.BasicDBObject;
import com.it21learning.ingestion.common.Tuple;

//mongo parser
public class EventAttendeesParser extends com.it21learning.ingestion.data.EventAttendeesParser<Tuple<BasicDBObject, BasicDBObject>> {
    //parse the record
    public Tuple<BasicDBObject, BasicDBObject> parse(String[] fields) {
        BasicDBObject d = new BasicDBObject();

        //event_id
        d.put("event_id", fields[0]);
        //user_id
        d.put("user_id", fields[1]);
        //attend_type
        d.put("attend_type", fields[2]);

        //the key & doc are the same
        return new Tuple<>(d, d);
    }
}

