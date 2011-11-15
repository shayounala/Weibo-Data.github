/**
 * 
 */
package test;

import java.net.UnknownHostException;
import java.util.ArrayList;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

/**
 * @author Administrator
 *
 */
public class Process {

	/**
	 * @param args
	 */
	@SuppressWarnings({ "unused", "unchecked" })
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Mongo mongo;
		try {
			mongo = new Mongo("localhost",27017);
			DB db = mongo.getDB("testdb");
			DBCollection userinformation = db.getCollection("UserInformation");
			DBCollection userinformationfilter = db.getCollection("UserInformationFilter");
			userinformationfilter.drop();
			
			ArrayList<Integer> uniqueusers = (ArrayList<Integer>) userinformation.distinct("ID");
			System.out.println("finish the unique reading: "+uniqueusers.size());
			//System.exit(0);
			int totalnumber = 0,totalnumberfilter = 0;
			ArrayList<Integer> followers;
			BasicDBObject object,query,update;
			for(int i=0;i<uniqueusers.size();i++){
				System.out.println(System.currentTimeMillis());
				object = (BasicDBObject) userinformation.findOne(new BasicDBObject("ID",uniqueusers.get(i)));
				followers = (ArrayList<Integer>) object.get("Followers ID");
				System.out.println(followers.size());
				for(int j=0;j<followers.size();j++){
					if(uniqueusers.contains(followers.get(j))){
						
					}else{
						followers.remove(j);
						j--;
					}
				}
				System.out.println(System.currentTimeMillis());
				totalnumber += followers.size();
				query = new BasicDBObject("ID",uniqueusers.get(i));
				update = new BasicDBObject();
				update.put("ID", uniqueusers.get(i));
				update.put("Followers ID", followers);
				System.out.println(followers.size());
				userinformation.update(query, update);
				System.out.println(System.currentTimeMillis());
				if(followers.size()==0){
					userinformation.remove(update);
				}
				if(followers.size()>=5 && followers.size()<=500){
					totalnumberfilter += followers.size();
					userinformationfilter.insert(update);
				}
				System.out.println(i);
			}
			
			
			System.out.println("totalnumber"+totalnumber);
			System.out.println("Number of Unique Users: "+userinformationfilter.distinct("ID").size());
			System.out.println("totalnumberfilter"+totalnumberfilter);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MongoException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

	}

}
