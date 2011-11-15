/**
 * 
 */
package test;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;

import com.mongodb.BasicDBObject;
import com.mongodb.Bytes;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.QueryOperators;

/**
 * @author Administrator
 * 
 */
public class MongoTest {

	/**
	 * @param args
	 */
	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//testtheMongo();
		testMiningResults();
		
		

	}

	private static void testMiningResults() {
		// TODO Auto-generated method stub
		try {
			Mongo mongo = new Mongo("10.3.4.84",27017);
			DB db = mongo.getDB("mydb");
			System.out.println(db.authenticate("cssc", new char[]{'1'}));
			DBCollection dbcollection = db.getCollection("UniqueUserIDs");
			
			
			DBCursor cursor = dbcollection.find(new BasicDBObject("User ID",
					new BasicDBObject(QueryOperators.EXISTS, true)));
			cursor.setOptions(Bytes.QUERYOPTION_NOTIMEOUT);
			System.out.println("Count of the Collection: "+cursor.count());
			System.out.println(new Date());
			ArrayList<Long> UniqueUserIDs = new ArrayList<Long>();
			for(int i=0;i<cursor.count();i++){
				UniqueUserIDs.addAll((Collection<? extends Long>) cursor.next().get("User ID"));
			}
			
			System.out.println("The number of the Unique User IDs: "+UniqueUserIDs.size());
			System.out.println("The number of followers IDs: "+db.getCollection("UserInformation").find(new BasicDBObject("Followers ID",new BasicDBObject(QueryOperators.EXISTS,true))).count());
			System.exit(0);
			dbcollection.drop();
			ArrayList<Long> grandIDs = new ArrayList<Long>();
			for(int i=0;i<1000000;i++){
				grandIDs.add(UniqueUserIDs.get(i));
				if(i%10000==9999){
					dbcollection.insert(new BasicDBObject("User ID",grandIDs));
					grandIDs.clear();
				}
			}
			
			
			cursor = dbcollection.find(new BasicDBObject("User ID",
					new BasicDBObject(QueryOperators.EXISTS, true)));
			cursor.setOptions(Bytes.QUERYOPTION_NOTIMEOUT);
			System.out.println(new Date());
			ArrayList<Long> results = new ArrayList<Long>();
			for(int i=0;i<cursor.count();i++){
				results.addAll((Collection<? extends Long>) cursor.next().get("User ID"));
			}
			
			System.out.println(results.size());
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MongoException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	/**
	 * test the ability for Mongo DB
	 */
	private static void testtheMongo() {
		// TODO Auto-generated method stub
		try {
			Mongo mongo = new Mongo("localhost", 27017);
			DB db = mongo.getDB("mongotest");
			DBCollection dbcollection = db.getCollection("mongotest");
			dbcollection.drop();
			BasicDBObject [] object = new BasicDBObject [10000];
			ArrayList<Long> [] array = new ArrayList [10000];

			for(int i=0;i<array.length;i++){
				array[i] =  new ArrayList<Long>();
				for(int j=0;j<100;j++){
					array[i].add(Long.valueOf((long)(i*10+j)));
				}
			}
			
			for(int i=0;i<object.length;i++){
				object[i] = new BasicDBObject("ID",array[i]);
				object[i].append("number", i);
				dbcollection.insert(object[i]);
			}
			
			
			DBCursor cursor = dbcollection.find(new BasicDBObject("ID",
					new BasicDBObject(QueryOperators.EXISTS, true)));
			ArrayList<Long> results = new ArrayList<Long>();
			cursor.setOptions(Bytes.QUERYOPTION_NOTIMEOUT);
			System.out.println(new Date());
			for(int i=0;i<cursor.count();i++){
				results.addAll((Collection<? extends Long>) cursor.next().get("ID"));
			}
			System.out.println(results.size()+" Last Number: "+results.get(results.size()-1));
			System.out.println(dbcollection.getCount());
			System.out.println(new Date());
			
			
			results.clear();
			for(int i=0;i<cursor.count();i++){
				results.addAll((Collection<? extends Long>) dbcollection.findOne(new BasicDBObject("number",i)).get("ID"));
			}
			System.out.println(results.size()+" Last Number: "+results.get(results.size()-1));
			System.out.println(dbcollection.getCount());
			System.out.println(new Date());
			
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MongoException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
