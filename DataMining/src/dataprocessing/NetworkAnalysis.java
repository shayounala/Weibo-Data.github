package dataprocessing;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.QueryOperators;

public class NetworkAnalysis {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Mongo mongo;
		try {
			mongo = new Mongo("10.3.4.84", 27017);
			mongo.getDB("weibodb").authenticate("cssc", new char[] { '1' });
			DB dbFiltered = mongo.getDB("weibodb");
			mongo.getDB("db").authenticate("cssc", new char[] { '1' });
			DB db = mongo.getDB("db");
			DBCollection  collection = dbFiltered.getCollection("UserInformation");
			DBCollection  filtercollection = dbFiltered.getCollection("FilterUserInformation");
			DBCollection  regularcollection = dbFiltered.getCollection("RegularUserInformation");
			DBCollection  regulatedcollection = db.getCollection("RegulatedUserInformation");
			//transferDB(db,dbFiltered);
			//System.exit(0);
			//followersanalysis(regulatedcollection, "Undirected Followers IDs");
			calculateCE(regulatedcollection, "Undirected Followers IDs");
			//directedanalysis(regularcollection,regulatedcollection);
			//RegulateInformation(filtercollection, regularcollection);
			//showcollection(regulatedcollection);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MongoException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

	}

	private static double calculateCE(DBCollection regulatedcollection,
			String string) {
		// TODO Auto-generated method stub
		ArrayList<Long> users = getUsers(regulatedcollection);
		ArrayList<ArrayList> followers = getFollowers(regulatedcollection, string);
		
		if(string == "Directed Followers IDs"){
			for(int i=0;i<followers.size();i++){
				long user = users.get(i);
				ArrayList<Long> followerids = followers.get(i);
				
				for(int j=0;j<followerids.size();j++){
					ArrayList<Long> temp = followers.get(users.indexOf(followerids.get(j)));
					if(!temp.contains(user)){
						temp.add(user);
					}
				}
			}
			
			
			int randomint = Long.bitCount(System.currentTimeMillis())%followers.size();
			
			while(followers.get(randomint).size() == 0){
				randomint = (randomint+1)%followers.size();
			}
			
			ArrayList<Long> temp = followers.get(users.indexOf(followers.get(randomint).get(0)));
			long tempuser = users.get(randomint);
			if(!temp.contains(tempuser)){
				System.out.println("Exception in transferring directed network to undirected network");
				System.exit(0);
			}
		}
		
		return calculateCE(users, followers);
		
	}

	private static double calculateCE(ArrayList<Long> users,
			ArrayList<ArrayList> followers) {
		// TODO Auto-generated method stub
		double totaltriplets = 0, closedtriplets = 0;
		
		for(int i = 0; i<followers.size(); i++){
			ArrayList<Long> followerids = followers.get(i);
			
			for(int j=0; j<followerids.size();j++){
				for(int k=j;k<followerids.size();k++){
					if(k!=j){
						ArrayList<Long> temp = followers.get(users.indexOf(followerids.get(j)));
						if(temp.contains(followerids.get(k))){
							totaltriplets++;
							closedtriplets++;
						}else{
							totaltriplets += 3;
						}
					}
				}
			}
		}
		
		double CE = closedtriplets/totaltriplets;
		
		return CE;
	}

	private static ArrayList<ArrayList> getFollowers(DBCollection collection,
			String string) {
		// TODO Auto-generated method stub
		DBCursor cursor = collection.find(new BasicDBObject("User IDs", new BasicDBObject(QueryOperators.EXISTS, true)));
		
		ArrayList<ArrayList> followers = new ArrayList<ArrayList> ();
		ArrayList<Long> follower = new ArrayList<Long> ();
		ArrayList<Long> followerids = new ArrayList<Long> ();
		
		for(int i=0;cursor.hasNext();i++){
			BasicDBObject object = (BasicDBObject) cursor.next();
			follower = (ArrayList<Long>) object.get(string);
			for(int j=0;j<follower.size();j++){
				long followerid = follower.get(j);
				if(followerid == -1){
					followers.add((ArrayList) followerids.clone());
					followerids.clear();
				}else{
					followerids.add(followerid);
				}
			}
			
			follower.clear();
			System.out.println(i);
		}
		
		System.out.println("Size of User: "+followers.size());
		
		int number = 0;
		for(ArrayList follow : followers){
			number += follow.size();
		}
		System.out.println("Size of Followers: "+number);
		return followers;
	}

	private static void transferDB(DB dbFiltered, DB db) {
		// TODO Auto-generated method stub
		db.dropDatabase();
		DBCollection  collection = dbFiltered.getCollection("UserInformation");
		DBCollection  filtercollection = dbFiltered.getCollection("FilterUserInformation");
		DBCollection  regularcollection = dbFiltered.getCollection("RegularUserInformation");
		
		DBCollection  collection1 = db.getCollection("UserInformation");
		DBCollection  filtercollection1 = db.getCollection("FilterUserInformation");
		DBCollection  regularcollection1 = db.getCollection("RegularUserInformation");
		
		DBCursor cursor = collection.find();
		while(cursor.hasNext()){
			BasicDBObject object = (BasicDBObject) cursor.next();
			collection1.insert(object);
		}
		
		cursor = filtercollection.find();
		while(cursor.hasNext()){
			BasicDBObject object = (BasicDBObject) cursor.next();
			filtercollection1.insert(object);
		}
		
		cursor = regularcollection.find();
		while(cursor.hasNext()){
			BasicDBObject object = (BasicDBObject) cursor.next();
			regularcollection1.insert(object);
		}
		
	}

	private static void showcollection(DBCollection collection) {
		// TODO Auto-generated method stub
		
		getUsers(collection);
		//getFollowers(collection);
		DBCursor cursor = collection.find(new BasicDBObject("Number", new BasicDBObject(QueryOperators.EXISTS, true)));
		System.out.println("Size of "+collection.getName()+": "+cursor.count());
		
		int totalnumber = 0, directednumber = 0, undirectednumber = 0;
		for(int i=0;cursor.hasNext();i++){
			
			BasicDBObject object = (BasicDBObject) cursor.next();
			System.out.println("User Number: "+object.get("Number"));
			
			ArrayList<Long> totalfollowers = (ArrayList<Long>) object.get("Total Followers IDs");
			ArrayList<Long> directedfollowers = (ArrayList<Long>) object.get("Directed Followers IDs");
			ArrayList<Long> undirectedfollowers = (ArrayList<Long>) object.get("Undirected Followers IDs");
			
			totalnumber +=totalfollowers.size();
			directednumber += directedfollowers.size();
			undirectednumber += undirectedfollowers.size();
				
		}
		
		System.out.println("Size of total followers: "+totalnumber);
		System.out.println("Size of directed followers: "+directednumber);
		System.out.println("Size of undirected followers: "+undirectednumber);
		
		collection.drop();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static void directedanalysis(DBCollection collection, DBCollection regulatedcollection) {
		// TODO Auto-generated method stub

		ArrayList<Long> Users = getUsers(collection);

		ArrayList<ArrayList> followers = getFollowers(collection);
		
		ArrayList<ArrayList> totalfollowers = new ArrayList<ArrayList>();
		ArrayList<ArrayList> directedfollowers = new ArrayList<ArrayList>();
		ArrayList<ArrayList> undirectedfollowers = new ArrayList<ArrayList>();
		ArrayList<Long> temp = new ArrayList<Long>();
		ArrayList<Long> total = new ArrayList<Long>();
		ArrayList<Long> directed = new ArrayList<Long>();
		ArrayList<Long> undirected = new ArrayList<Long>();
		int totalnumber = 0,directednumber = 0,undirectednumber = 0;
		for(int i=0;i<followers.size();i++){
			//System.out.println("User number: "+i);
			temp = followers.get(i);
			for(int j=0;j<temp.size();j++){
				long id = temp.get(j);
				int index = Users.indexOf(id);
				
				if(index == -1){
					
				}else{
					total.add(id);
					if(followers.get(index).contains(Users.get(i))){
						undirected.add(id);
					}else{
						directed.add(id);
					}
				}
				
			}
			
			totalfollowers.add((ArrayList) total.clone());
			directedfollowers.add((ArrayList) directed.clone());
			undirectedfollowers.add((ArrayList) undirected.clone());
/*			System.out.println(temp.size());
			System.out.println(directed.size());
			System.out.println(undirected.size());
			System.out.println(useless.size());*/
			totalnumber += total.size();
			directednumber += directed.size();
			undirectednumber += undirected.size();
			temp.clear();
			total.clear();
			directed.clear();
			undirected.clear();
		}
		
		System.out.println("totalnumber: "+totalnumber);
		System.out.println("directednumber: "+directednumber);
		System.out.println("undirectednumber: "+undirectednumber);
		
		ArrayList<Long> user = new ArrayList<Long> ();
		ArrayList<Long> totalregularfollowers = new ArrayList<Long>();
		ArrayList<Long> directedregularfollowers = new ArrayList<Long>();
		ArrayList<Long> undirectedregularfollowers = new ArrayList<Long>();
		for(int i=0;i<totalfollowers.size();i++){
			
			//System.out.println("User number: "+i);
			user.add(Users.get(i));
			totalregularfollowers.addAll(totalfollowers.get(i));
			totalregularfollowers.add(-1L);
			directedregularfollowers.addAll(directedfollowers.get(i));
			directedregularfollowers.add(-1L);
			undirectedregularfollowers.addAll(undirectedfollowers.get(i));
			undirectedregularfollowers.add(-1L);
			
			
			if(i%100 == 99 || i == totalfollowers.size()-1){
				/*object.append("Total Followers IDs", totalregularfollowers);
				object.append("Directed Followers IDs",directedregularfollowers);
				object.append("Undirected Followers IDs", undirectedregularfollowers);
				collection.update(new BasicDBObject("Number", Number), object);
				
				BasicDBObject testobject = (BasicDBObject) collection.findOne(new BasicDBObject("Number",Number));
				if(testobject == null){
					System.out.println(Number+"  "+totalregularfollowers.size());
				}else{
					ArrayList<Long> testfollowers = (ArrayList<Long>) testobject.get("Total Followers IDs");
					System.out.println(totalregularfollowers.size()+"   "+testfollowers.size());
				}
				
				totalregularfollowers.clear();
				directedregularfollowers.clear();
				undirectedregularfollowers.clear();*/
				System.out.println("Number: "+regulatedcollection.count());
				BasicDBObject totalobject = new BasicDBObject();
				totalobject.append("Number", regulatedcollection.count()+1);
				totalobject.append("User IDs", user);
				
				totalobject.append("Total Followers IDs", totalregularfollowers);
				totalobject.append("Directed Followers IDs", directedregularfollowers);
				totalobject.append("Undirected Followers IDs", undirectedregularfollowers);
				
				
				regulatedcollection.insert(totalobject);
				
				user.clear();
				totalregularfollowers.clear();
				directedregularfollowers.clear();
				undirectedregularfollowers.clear();
				
			}
			
			
		}
		
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static ArrayList<ArrayList> getFollowers(DBCollection collection) {
		// TODO Auto-generated method stub
		return getFollowers(collection, "Followers IDs");
	}

	@SuppressWarnings("unchecked")
	private static ArrayList<Long> getUsers(DBCollection collection) {
		// TODO Auto-generated method stub
		DBCursor cursor = collection.find(new BasicDBObject("User IDs", new BasicDBObject(QueryOperators.EXISTS, true)));
		
		ArrayList<Long> Users = new ArrayList<Long>();
		for(int i=0;cursor.hasNext();i++){
			BasicDBObject object = (BasicDBObject) cursor.next();
			Users.addAll((Collection<? extends Long>) object.get("User IDs"));
			System.out.println(i+"  "+Users.size());
		}
		
		System.out.println("Size of User: "+Users.size());
		return Users;
	}

	@SuppressWarnings("unchecked")
	private static void RegulateInformation(DBCollection collection1, DBCollection collection2) {
		// TODO Auto-generated method stub
		collection2.drop();
		System.out.println(collection2.count());
		DBCursor cursor = collection1.find(new BasicDBObject("User IDs", new BasicDBObject(QueryOperators.EXISTS, true)));

		ArrayList<Long> Users = new ArrayList<Long>();
		ArrayList<Long> Followers = new ArrayList<Long>();
		for(int i=0;cursor.hasNext();i++){
			BasicDBObject object = (BasicDBObject) cursor.next();
			Users.addAll((Collection<? extends Long>) object.get("User IDs"));
			Followers.addAll((Collection<? extends Long>) object.get("Followers IDs"));
		}
		
		System.out.println("Size of User: "+Users.size());
		System.out.println("Total Size of Follower: "+Followers.size());
		
		
		cursor = collection1.find(new BasicDBObject("User IDs", new BasicDBObject(QueryOperators.EXISTS, true)));
		
		ArrayList<Long> follower = new ArrayList<Long>();
		ArrayList<Long> user = new ArrayList<Long>();
		int tag = 0;
		for(int i=0;i<Followers.size();i++){
			long followerid = Followers.get(i);
			follower.add(followerid);
			
			if(followerid == -1){
				user.add(Users.get(tag));
				tag++;
				if(tag%100 == 0 && tag>0){
					BasicDBObject information = new BasicDBObject();
					information.append("Number", collection2.count()+1);
					information.append("User IDs", user);
					information.append("Followers IDs", follower);
					
					collection2.insert(information);
					System.out.println(collection2.count()+"  situation1  "+tag+"  "+user.size()+"   "+follower.size());
					
					user.clear();
					follower.clear();
				}else if(tag == Users.size()){
					BasicDBObject information = new BasicDBObject();
					information.append("Number", collection2.count()+1);
					information.append("User IDs", user);
					information.append("Followers IDs", follower);
					
					collection2.insert(information);
					System.out.println(collection2.count()+"  situation2  "+tag);
					
					user.clear();
					follower.clear();
				}
			}
		}
		
		
		cursor = collection2.find(new BasicDBObject("User IDs", new BasicDBObject(QueryOperators.EXISTS, true)));

		ArrayList<Long> Users1 = new ArrayList<Long>();
		for(int i=0;cursor.hasNext();i++){
			BasicDBObject object = (BasicDBObject) cursor.next();
			Users1.addAll((Collection<? extends Long>) object.get("User IDs"));
		}
		
		System.out.println(Users1.size());
	}

	@SuppressWarnings("unchecked")
	private static void followersanalysis(DBCollection collection, String followerkey) {
		// TODO Auto-generated method stub
		DBCursor cursor = collection.find(new BasicDBObject("User IDs", new BasicDBObject(QueryOperators.EXISTS, true)));
		
		int usernumber = 0;
		int userfollower = 0;
		int userfilterfollower = 0;
		int followerdistribution [] = new int [10000];
		for(int i=0;i<followerdistribution.length;i++){
			followerdistribution [i] = 0;
		}
		
		int totalnumber = 0;
		
		for(int i=0;cursor.hasNext();i++){
			BasicDBObject object = (BasicDBObject) cursor.next();
			ArrayList<Long> users = (ArrayList<Long>) object.get("User IDs");
			ArrayList<Long> followers = (ArrayList<Long>) object.get(followerkey);
			
			totalnumber += followers.size();
			usernumber = usernumber+users.size();
			
			ArrayList<Long> follower = new ArrayList<Long>();
			
			for(int j=0; j<followers.size();j++){
				long followerid = followers.get(j);
				follower.add(followers.get(j));
				
				if(followerid == -1){
					if(follower.size()>=2){
						userfilterfollower += 1;
						followerdistribution [follower.size()-2] += 1;
					}
					userfollower += 1;
					follower.clear();
				}
				
			}
		}
		
		System.out.println("Size of users: "+usernumber);
		System.out.println("Size of users for followers: "+userfollower);
		System.out.println("Size of filtered users for followers: "+userfilterfollower);
		
		
		for(int i=0;i<followerdistribution.length;i++){
			//System.out.println(i+"   "+followerdistribution[i]);
/*			if(followerdistribution[i]!=0){
				System.out.println(i);
			}*/
			System.out.println(Math.log(followerdistribution[i]+1)/Math.log(10));
			//System.out.println(Math.log(i+1)/Math.log(10));
		}
		
		System.out.println("Total number of followers: "+totalnumber);
	}

}
