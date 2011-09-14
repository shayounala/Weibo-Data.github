package dataprocessing;

import java.net.UnknownHostException;
import java.util.ArrayList;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
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
		mongo.dropDatabase("mydb");
		DB db = mongo.getDB("mydb");
		
		DBCollection dbcollection = db.getCollection("testcollection");
		for(int i=0;i<100;i++){
			BasicDBObject id = new BasicDBObject("ID",i);
			int [] idnumbers = new int[100];
			for(int j=0;j<100;j++){
				idnumbers[j] = i+j;
			}
			id.put("idnumber", idnumbers);
			dbcollection.insert(id);
		}
		
		DBCursor dbcursor = dbcollection.find(new BasicDBObject("idnumber1",new BasicDBObject(QueryOperators.EXISTS,true)));
		System.out.println(dbcollection.distinct("idnumber",new BasicDBObject("ID",55)));
		
		
	}

}
