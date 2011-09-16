package dataprocessing;

import java.net.UnknownHostException;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.QueryOperators;

public class ReadFromMongo {

	/**
	 * @param args
	 * @throws MongoException 
	 * @throws UnknownHostException 
	 */
	public static void main(String[] args) throws UnknownHostException, MongoException {
		// TODO Auto-generated method stub
		Mongo mongo = new Mongo("localhost",27017);
		//mongo.dropDatabase("mydb");
		DB db = mongo.getDB("testdb");
		
		DBCollection dbcollection = db.getCollection("UserInformation");
		System.out.println(dbcollection.find(new BasicDBObject("ID",new BasicDBObject(QueryOperators.EXISTS,true))).count());
		
		int nextid = dbcollection.find(new BasicDBObject("ID",new BasicDBObject(QueryOperators.EXISTS,true))).count();
		DBObject object = db.getCollection("NextIDs").findOne(new BasicDBObject("Next ID for Mining followers ID",new BasicDBObject(QueryOperators.EXISTS,true)));
		object.put("Next ID for Mining followers ID", nextid);
		db.getCollection("NextIDs").update(new BasicDBObject("Next ID for Mining followers ID",new BasicDBObject(QueryOperators.EXISTS,true)), object);
		System.out.println(db.getCollection("NextIDs").findOne());
	}

}
