package com.it21learning.ingestion.data.mongo;

import com.mongodb.BasicDBObject;
import com.it21learning.ingestion.common.Tuple;

//mongo parser
public class TrainParser extends com.it21learning.ingestion.data.TrainParser<Tuple<BasicDBObject, BasicDBObject>> {
    //parse the record
    public Tuple<BasicDBObject, BasicDBObject> parse(String[] fields) {
        //the key
        BasicDBObject k = new BasicDBObject();
        //user
        k.put("user", fields[0]);
        //event
        k.put("event", fields[1]);

        //doc
        BasicDBObject d = new BasicDBObject();
        //user
        d.put("user", fields[0]);
        //event
        d.put("event", fields[1]);

        //invited
        d.put("invited", fields[2]);
        //time_stamp
        d.put("time_stamp", fields[3]);
        //interested
        d.put("interested", fields[4]);
        //not_interested
        d.put("not_interested", fields[5]);

        //result
        return new Tuple<>(k, d);
    }
}