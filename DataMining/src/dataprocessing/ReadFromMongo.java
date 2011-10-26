package dataprocessing;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;

import com.mongodb.Mongo;
import com.mongodb.MongoException;




public class ReadFromMongo {

	/**
	 * @param args
	 * @throws MongoException 
	 * @throws UnknownHostException 
	 */
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws UnknownHostException, MongoException {
		// TODO Auto-generated method stu
		
		Mongo mongo = new Mongo("localhost",27017);
		DB db = mongo.getDB("mydb");
		//System.out.println(db.authenticate("cssc", new char []{'1'}));
		
		DBCollection AccountInformationCollection = db.getCollection("AccountInformation");
		
		
		
		
		ArrayList<String> weiboaccount = (ArrayList<String>) AccountInformationCollection.distinct("weiboAccount");
		ArrayList<String> weibopassword = (ArrayList<String>) AccountInformationCollection.distinct("weiboPassword");
		ArrayList<String> weibotoken = (ArrayList<String>) AccountInformationCollection.distinct("weiboToken");
		ArrayList<String> weibosecrettoken = (ArrayList<String>) AccountInformationCollection.distinct("weiboTokenSecret");
	
		DataInputStream accountinfos;
		try {
			accountinfos = new DataInputStream(new FileInputStream("AccountInformation.log"));
			String start = accountinfos.readLine();
			int number = 0;
			while(start!=null){
				switch(number%4){
				case 0:
					weiboaccount.add(start);
					break;
				case 1:
					weibopassword.add(start);
					break;
				case 2:
					weibotoken.add(start);
					break;
				case 3:
					weibosecrettoken.add(start);
					break;
					}
				number++;
				start = accountinfos.readLine();
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		for(int i=0;i<weiboaccount.size();i++){
			System.out.println(weiboaccount.get(i));
			System.out.println(weibopassword.get(i));
			System.out.println(weibotoken.get(i));
			System.out.println(weibosecrettoken.get(i));
		}
		
		
		for(int i=0;i<weiboaccount.size();i++){
			BasicDBObject accountinfo = new BasicDBObject();
			accountinfo.append("weiboAccount", weiboaccount.get(i));
			accountinfo.append("weiboPassword", weibopassword.get(i));
			accountinfo.append("weiboToken", weibotoken.get(i));
			accountinfo.append("weiboTokenSecret", weibosecrettoken.get(i));
			AccountInformationCollection.insert(accountinfo);
		}
		
		
		
		weiboaccount = (ArrayList<String>) AccountInformationCollection.distinct("weiboAccount");
		weibopassword = (ArrayList<String>) AccountInformationCollection.distinct("weiboPassword");
		weibotoken = (ArrayList<String>) AccountInformationCollection.distinct("weiboToken");
		weibosecrettoken = (ArrayList<String>) AccountInformationCollection.distinct("weiboTokenSecret");
		
		for(int i=0;i<weiboaccount.size();i++){
			System.out.println(weiboaccount.get(i));
			System.out.println(weibopassword.get(i));
			System.out.println(weibotoken.get(i));
			System.out.println(weibosecrettoken.get(i));
		}
		
	}

}
