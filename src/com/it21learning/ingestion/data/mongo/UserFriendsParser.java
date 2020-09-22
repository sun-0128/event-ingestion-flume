package com.it21learning.ingestion.data.mongo;

import com.mongodb.BasicDBObject;
import com.it21learning.ingestion.common.Tuple;

//mongo parser
public class UserFriendsParser extends com.it21learning.ingestion.data.UserFriendsParser<Tuple<BasicDBObject, BasicDBObject>> {
    //parse the record
    public Tuple<BasicDBObject, BasicDBObject> parse(String[] fields) {
        //the doc
        BasicDBObject d = new BasicDBObject();

        //user_id
        d.put("user_id", fields[0]);
        //friend_id
        d.put("friend_id", fields[1]);

        //result
        return new Tuple<>(d, d);
    }
}
