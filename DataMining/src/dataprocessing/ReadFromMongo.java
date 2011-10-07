package dataprocessing;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.QueryOperators;

import datamining.Mining;

public class ReadFromMongo {

	/**
	 * @param args
	 * @throws MongoException 
	 * @throws UnknownHostException 
	 */
	public static void main(String[] args) throws UnknownHostException, MongoException {
		// TODO Auto-generated method stu
		
		Mongo mongo = new Mongo("10.3.4.84",27017);
		DB db = mongo.getDB("mydb");
		//db.dropDatabase();
		System.out.println(db.authenticate("cssc", new char []{'1'}));
		//db.addUser("seu", new char []{'1'});
		
		DBCollection AccountInformationCollection = db.getCollection("AccountInformation");
		ArrayList<String> weiboaccount = (ArrayList<String>) AccountInformationCollection.distinct("weiboAccount");
		ArrayList<String> weibopassword = (ArrayList<String>) AccountInformationCollection.distinct("weiboPassword");
		ArrayList<String> weibotoken = (ArrayList<String>) AccountInformationCollection.distinct("weiboToken");
		ArrayList<String> weibosecrettoken = (ArrayList<String>) AccountInformationCollection.distinct("weiboTokenSecret");
	
		for(int i=0;i<weiboaccount.size();i++){
			System.out.println(weiboaccount.get(i));
			System.out.println(weibopassword.get(i));
			System.out.println(weibotoken.get(i));
			System.out.println(weibosecrettoken.get(i));
		}
	}

}
