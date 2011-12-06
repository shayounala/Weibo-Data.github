package dataprocessing;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;

import com.mongodb.BasicDBObject;
import com.mongodb.Bytes;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
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
			DBCollection  collection = db.getCollection("UserInformation");
			DBCollection  filtercollection = db.getCollection("FilterUserInformation");
			DBCollection  regularcollection = db.getCollection("RegularUserInformation");
			DBCollection  regulatedcollection = db.getCollection("RegulatedUserInformation");
			DBCollection  anonymouscollection = db.getCollection("AnonymousUserInformation");
			DBCollection directedcollection = db.getCollection("DirectedUserInformation");
			DBCollection directedanonymouscollection = db.getCollection("DirectedAnonymousUserInformation");
			DBCollection  uniqueuseridscollection = db.getCollection("UniqueUserIDs");
			DBCollection tweetinformationcollection = db.getCollection("TweetInformation");
			
			//FilterFollowers(regularcollection, regulatedcollection);
			//RegulateInformation(regularcollection, regulatedcollection);
			//anonymitythedata(regulatedcollection, anonymouscollection);
			//directedanalysis(regulatedcollection,directedcollection);
			//directedanonymousanalysis(anonymouscollection,directedanonymouscollection);
			//AbstractUniqueUserIDs(regulatedcollection,uniqueuseridscollection);
			analysistweetinformation(tweetinformationcollection);
			
			
			//transferDB(dbFiltered, db);
				
			//calculateCE(directedanonymouscollection, "Undirected Followers IDs");
			//followersanalysis(regulatedcollection, "Undirected Followers IDs");
			//showcollection(anonymouscollection);
			//comparecollection(regulatedcollection,anonymouscollection);
			
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MongoException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

	}
	
	
	private static void analysistweetinformation(
			DBCollection tweetinformationcollection) {
		// TODO Auto-generated method stub
		DBCursor cursor = tweetinformationcollection.find();
		cursor.setOptions(Bytes.QUERYOPTION_NOTIMEOUT);
		System.out.println("Size of TweetInformation: "+cursor.count());
		
		for(int i=0;cursor.hasNext();i++){
			BasicDBObject object = (BasicDBObject) cursor.next();
			object.get("Tweets ID");
			object.get("Reposts ID");
		}
		
	}


	private static void AbstractUniqueUserIDs(DBCollection regulatedcollection,
			DBCollection uniqueuseridscollection) {
		// TODO Auto-generated method stub
		uniqueuseridscollection.drop();
		
		ArrayList<Long> users = getUsers(regulatedcollection);
		ArrayList<Long> userids = new ArrayList<Long>();
		
		for(int i=0;i<users.size();i++){
			userids.add(users.get(i));
			if(i != users.size()-1){
				if(i%1000 == 999 ){
					BasicDBObject object = new BasicDBObject("User ID", userids);
					uniqueuseridscollection.insert(object);
					userids.clear();
				}
			}else{
				BasicDBObject object = new BasicDBObject("User ID", userids);
				uniqueuseridscollection.insert(object);
				userids.clear();
			}
		}
		
		ArrayList<Long> UniqueUserIDsList = new ArrayList<Long>();
		
		DBCursor cursor = uniqueuseridscollection.find(new BasicDBObject("User ID",
				new BasicDBObject(QueryOperators.EXISTS, true)));
		for(int i=0;i<cursor.count();i++){
			UniqueUserIDsList.addAll((Collection<? extends Long>) cursor.next().get("User ID"));
		}

		System.out.println("Number of Unique User IDs: "+UniqueUserIDsList.size());
	}


	@SuppressWarnings({ "unchecked" })
	private static void comparecollection(DBCollection regulatedcollection,
			DBCollection anonymouscollection) {
		// TODO Auto-generated method stub
		DBCursor regulatedcursor = regulatedcollection.find();
		DBCursor anonymouscursor = anonymouscollection.find();
		System.out.println(regulatedcollection.getFullName()+": "+regulatedcursor.count());
		System.out.println(anonymouscollection.getFullName()+": "+anonymouscursor.count());
		
		//regulatedcursor.sort(new BasicDBObject("Number", new BasicDBObject(QueryOperators.EXISTS, true)));
		//anonymouscursor.sort(new BasicDBObject("Number", new BasicDBObject(QueryOperators.EXISTS, true)));

		
		for(int i=0; regulatedcursor.hasNext();i++){
			BasicDBObject object1 = (BasicDBObject) regulatedcursor.next();
			BasicDBObject object2 = (BasicDBObject) anonymouscursor.next();
			
			int number1 = (Integer) object1.get("Number");
			int number2 = (Integer) object2.get("Number");
			
			ArrayList<Long> followers1 = (ArrayList<Long>) object1.get("Followers IDs");
			ArrayList<Integer> followers2 = (ArrayList<Integer>) object2.get("Followers IDs");
			
			ArrayList<Long> users1 = (ArrayList<Long>) object1.get("User IDs");
			ArrayList<Integer> users2 = (ArrayList<Integer>) object2.get("User IDs");
			
			if(number1 != number2){
				System.out.println("Something is wrong in the process of anonymity"+"number");
				System.out.println(i);
				System.out.println("number1.: "+number1+"  "+"number2: "+number2);
				System.out.println("users2.start: "+users2.get(0)+"  "+"users2.end "+users2.get(users2.size()-1));
			}
			
			if(followers1.size() != followers2.size()){
				System.out.println("Something is wrong in the process of anonymity"+"followers");
				System.out.println(i);
				System.out.println("followers1.: "+followers1.size()+"  "+"followers2: "+followers2.size());
			}
			
			if(users1.size() != users2.size()){
				System.out.println("Something is wrong in the process of anonymity"+"users");
				System.out.println(i);
				System.out.println("users1.: "+users1.size()+"  "+"users2: "+users2.size());
			}
			
			//System.out.println("number1.: "+number1+"  "+"number2: "+number2);
			//System.out.println("followers1.: "+followers1.size()+"  "+"followers2: "+followers2.size());
		}
	}


	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static void directedanonymousanalysis(
			DBCollection anonymouscollection, DBCollection directedcollection) {
		// TODO Auto-generated method stub
		directedcollection.drop();
		
		ArrayList<Integer> users = getUserkeys(anonymouscollection);

		ArrayList<ArrayList> followers = getFollowerkeys(anonymouscollection, "Followers IDs");
		
		ArrayList<ArrayList> totalfollowers = new ArrayList<ArrayList>();
		ArrayList<ArrayList> directedfollowers = new ArrayList<ArrayList>();
		ArrayList<ArrayList> undirectedfollowers = new ArrayList<ArrayList>();
		ArrayList<Integer> temp = new ArrayList<Integer>();
		ArrayList<Integer> total = new ArrayList<Integer>();
		ArrayList<Integer> directed = new ArrayList<Integer>();
		ArrayList<Integer> undirected = new ArrayList<Integer>();
		int totalnumber = 0,directednumber = 0,undirectednumber = 0, uselessnumber = 0;
		for(int i=0;i<followers.size();i++){
			System.out.println("User number: "+i);
			temp = followers.get(i);
			for(int j=0;j<temp.size();j++){
				int id = temp.get(j);
				
				if(id == -1){
					uselessnumber++;
				}else{
					total.add(id);
					if(followers.get(id).contains(i)){
						undirected.add(id);
					}else{
						directed.add(id);
					}
				}
				
			}
			
			totalfollowers.add((ArrayList) total.clone());
			directedfollowers.add((ArrayList) directed.clone());
			undirectedfollowers.add((ArrayList) undirected.clone());

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
		System.out.println("uselessnumber: "+uselessnumber);
		
		ArrayList<Integer> user = new ArrayList<Integer> ();
		ArrayList<Integer> totalregularfollowers = new ArrayList<Integer>();
		ArrayList<Integer> directedregularfollowers = new ArrayList<Integer>();
		ArrayList<Integer> undirectedregularfollowers = new ArrayList<Integer>();
		for(int i=0;i<totalfollowers.size();i++){
			
			//System.out.println("User number: "+i);
			user.add(i);
			totalregularfollowers.addAll(totalfollowers.get(i));
			totalregularfollowers.add(-1);
			directedregularfollowers.addAll(directedfollowers.get(i));
			directedregularfollowers.add(-1);
			undirectedregularfollowers.addAll(undirectedfollowers.get(i));
			undirectedregularfollowers.add(-1);
			
			
			if(i%100 == 99 || i == totalfollowers.size()-1){

				System.out.println("Number: "+directedcollection.count());
				BasicDBObject totalobject = new BasicDBObject();
				totalobject.append("Number", directedcollection.count()+1);
				totalobject.append("User IDs", user);
				
				totalobject.append("Total Followers IDs", totalregularfollowers);
				totalobject.append("Directed Followers IDs", directedregularfollowers);
				totalobject.append("Undirected Followers IDs", undirectedregularfollowers);
				
				
				directedcollection.insert(totalobject);
				
				user.clear();
				totalregularfollowers.clear();
				directedregularfollowers.clear();
				undirectedregularfollowers.clear();
				
			}
			
			
		}
	}



	@SuppressWarnings("unchecked")
	private static void FilterFollowers(DBCollection collection, DBCollection collection1) {
		// TODO Auto-generated method stub
		collection1.drop();
		
		DBCursor cursor = collection.find(new BasicDBObject("Followers IDs", new BasicDBObject(QueryOperators.EXISTS, true)));
		cursor.setOptions(Bytes.QUERYOPTION_NOTIMEOUT);
		
		ArrayList<Long> UniqueUsers = new ArrayList<Long>();
		for(int i=0;i<cursor.count();i++){
			ArrayList<Long> UniqueUser = (ArrayList<Long>) cursor.next().get("User IDs");
			UniqueUsers.addAll(UniqueUser);

		}
		System.out.println(cursor.count());
		cursor = collection.find(new BasicDBObject("Followers IDs", new BasicDBObject(QueryOperators.EXISTS, true)));
		ArrayList<Long> followers = new ArrayList<Long>();
		ArrayList<Long> users = new ArrayList<Long>();
		for(int i=0;i<cursor.count();i++){
			BasicDBObject object = (BasicDBObject) cursor.next();
			users =(ArrayList<Long>) object.get("User IDs");
			followers =(ArrayList<Long>) object.get("Followers IDs");
			for(int j=0;j<followers.size();j++){
				long followerid = followers.get(j).longValue();
				boolean contain = false;
				
				if(followerid != -1L){
					for(int k=0;k<UniqueUsers.size();k++){
						if(followerid == UniqueUsers.get(k)){
							contain =true;
							break;
						}
					}
				}else{
					contain = true;
				}
				
				if(!contain){
					followers.remove(followerid);
					j--;
				}
			}
			
			BasicDBObject information = new BasicDBObject();
			information.append("Number", i+1);
			information.append("User IDs", users);
			information.append("Followers IDs", followers);

			collection1.insert(information);
			object.clear();
			users.clear();
			followers.clear();
			System.out.println("Iretator of Filter Followers: "+i);
		}
		
	}

	@SuppressWarnings("unchecked")
	private static void anonymitythedata(DBCollection regularcollection,
			DBCollection anonymouscollection) {
		// TODO Auto-generated method stub
		anonymouscollection.drop();
		
		ArrayList<Long> users = getUsers(regularcollection);
		
		DBCursor cursor = regularcollection.find();
		
		for(int i=0;cursor.hasNext();i++){
			
			System.out.println("Curosr: "+i);
			
			BasicDBObject object = (BasicDBObject) cursor.next();
			
			BasicDBObject newobject = new BasicDBObject();
			
			int number = (Integer) object.get("Number");
			ArrayList<Long> user = (ArrayList<Long>) object.get("User IDs");
			ArrayList<Long> follower = (ArrayList<Long>) object.get("Followers IDs");
			
			ArrayList<Integer> newfollower = new ArrayList<Integer>();
			ArrayList<Integer> newuser = new ArrayList<Integer>();
			
			for(int j=0;j<user.size();j++){
				newuser.add(users.indexOf(user.get(j)));
			}
			for(int j=0;j<follower.size();j++){
				if(follower.get(j)!= -1){
					if(users.indexOf(follower.get(j)) == -1){
						System.exit(0);
					}
					newfollower.add(users.indexOf(follower.get(j)));
				}else{
					newfollower.add(-1);
				}
			}

			
			newobject.append("Number", number);
			newobject.append("User IDs", newuser);
			newobject.append("Followers IDs", newfollower);
			
			if(follower.size() != newfollower.size()){
				System.out.println("Number: "+number+" follower: "+follower.size()+" newnumber: "+i+" newfollower: "+newfollower.size());
				System.exit(0);
			}
			
			anonymouscollection.insert(newobject);
		}
		
		System.out.println("Make the user data anonymous");
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static double calculateCE(DBCollection directedcollection,
			String string) {
		// TODO Auto-generated method stub
		ArrayList<Integer> users = getUserkeys(directedcollection);
		ArrayList<ArrayList> followers = getFollowerkeys(directedcollection, string);
		
		if(string == "Directed Followers IDs"){
			
			System.out.println("Transferring directed network to undirected network");
			
			for(int i=0;i<followers.size();i++){
				int user = users.get(i);
				ArrayList<Integer> followerids = followers.get(i);
				
				for(int j=0;j<followerids.size();j++){
					ArrayList<Integer> temp = followers.get(followerids.get(j));
					if(!temp.contains(user)){
						temp.add(user);
					}
				}
			}
			
			
			int randomint = Long.bitCount(System.currentTimeMillis())%followers.size();
			
			while(followers.get(randomint).size() == 0){
				randomint = (randomint+1)%followers.size();
			}
			
			ArrayList<Integer> temp = followers.get(users.indexOf(followers.get(randomint).get(0)));
			int tempuser = users.get(randomint);
			if(!temp.contains(tempuser)){
				System.out.println("Exception in transferring directed network to undirected network");
				System.exit(0);
			}
		}
		
		//System.exit(0);
		return calculateCE(users, followers);
		
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static double calculateCE(ArrayList<Integer> users,
			ArrayList<ArrayList> followers) {
		// TODO Auto-generated method stub
		
		System.out.println("Calculate the clustering efficiency of undirected network");
		
		
		double totaltriplets = 0, closedtriplets = 0;
		int totalfollowers = 0;
		
		for(int i = 0; i<followers.size(); i++){
			
			ArrayList<Integer> followerids = followers.get(i);
			totalfollowers += followerids.size();
			
			System.out.println("User: "+i+" followerids: "+followerids.size());			
			
			for(int j=0; j<followerids.size();j++){
				for(int k=j;k<followerids.size();k++){
					if(k!=j){
						ArrayList<Integer> temp = followers.get(followerids.get(j));
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
		
		System.out.println("The clustering effiency of the network is: "+CE);
		System.out.println("Total Followers: "+totalfollowers+" Total Triplets: "+totaltriplets+" Closed Triplets: "+closedtriplets);
		
		return CE;
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

	@SuppressWarnings("unchecked")
	private static void showcollection(DBCollection collection) {
		// TODO Auto-generated method stub
		
		getUsers(collection);
		getFollowerkeys(collection, "Followers IDs");
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
		
		//collection.drop();
	}



	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static void directedanalysis(DBCollection collection, DBCollection regulatedcollection) {
		// TODO Auto-generated method stub

		regulatedcollection.drop();
		
		ArrayList<Long> Users = getUsers(collection);

		ArrayList<ArrayList> followers = getFollowers(collection);
		
		ArrayList<ArrayList> totalfollowers = new ArrayList<ArrayList>();
		ArrayList<ArrayList> directedfollowers = new ArrayList<ArrayList>();
		ArrayList<ArrayList> undirectedfollowers = new ArrayList<ArrayList>();
		ArrayList<Long> temp = new ArrayList<Long>();
		ArrayList<Long> total = new ArrayList<Long>();
		ArrayList<Long> directed = new ArrayList<Long>();
		ArrayList<Long> undirected = new ArrayList<Long>();
		int totalnumber = 0,directednumber = 0,undirectednumber = 0, uselessnumber = 0;
		for(int i=0;i<followers.size();i++){
			System.out.println("User number: "+i);
			temp = followers.get(i);
			for(int j=0;j<temp.size();j++){
				long id = temp.get(j);
				int index = Users.indexOf(id);
				
				if(index == -1){
					uselessnumber++;
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
		System.out.println("uselessnumber: "+uselessnumber);
		
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
	
	
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static ArrayList<Integer> getUserkeys(
			DBCollection anonymouscollection) {
		// TODO Auto-generated method stub
		DBCursor cursor = anonymouscollection.find(new BasicDBObject("User IDs", new BasicDBObject(QueryOperators.EXISTS, true)));
		
		ArrayList<Integer> Users = new ArrayList<Integer>();
		ArrayList<Integer> Numbers = new ArrayList<Integer>();
		ArrayList<ArrayList> user = new ArrayList<ArrayList>();
		for(int i=0;cursor.hasNext();i++){
			BasicDBObject object = (BasicDBObject) cursor.next();
			ArrayList<Integer> temp = (ArrayList) object.get("User IDs");
			user.add(temp);
			Numbers.add(temp.get(0)/100);
			System.out.println(i+"  "+Users.size());
			//System.out.println();
		}
		
		int [] numberorder = new int [Numbers.size()];
		numberorder = getNumberOrder(Numbers);
		
		for(int i=0;i<numberorder.length;i++){
			Users.addAll(user.get(numberorder[i]));
		}
		
		System.out.println("Size of User: "+Users.size());
		return Users;
	}
	
	


	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static ArrayList<ArrayList> getFollowers(DBCollection collection) {
		// TODO Auto-generated method stub
		return getFollowers(collection, "Followers IDs");
	}
	
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
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
	
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static ArrayList<ArrayList> getFollowerkeys(DBCollection collection, String string) {
		// TODO Auto-generated method stub
		DBCursor cursor = collection.find(new BasicDBObject("User IDs", new BasicDBObject(QueryOperators.EXISTS, true)));
		
		ArrayList<ArrayList> followers = new ArrayList<ArrayList> ();
		ArrayList<ArrayList> orderedfollowers = new ArrayList<ArrayList> ();
		ArrayList<Integer> follower = new ArrayList<Integer> ();
		ArrayList<Integer> followerids = new ArrayList<Integer> ();
		ArrayList<Integer> Numbers = new ArrayList<Integer>();
		
		for(int i=0;cursor.hasNext();i++){
			BasicDBObject object = (BasicDBObject) cursor.next();
			followers.add((ArrayList<Integer>) object.get(string));
			ArrayList<Integer> temp = (ArrayList) object.get("User IDs");
			Numbers.add(temp.get(0)/100);
		}
		
		int [] numberorder = new int [Numbers.size()];
		numberorder = getNumberOrder(Numbers);
		
		for(int i=0;i<numberorder.length;i++){
			follower = followers.get(numberorder[i]);
			for(int j=0;j<follower.size();j++){
				int followerid = follower.get(j);
				if(followerid == -1){
					orderedfollowers.add((ArrayList) followerids.clone());
					followerids.clear();
				}else{
					followerids.add(followerid);
				}
			}
			
			follower.clear();
			System.out.println(i);
		}
		
		//followers.clear();
		
		System.out.println("Size of User: "+orderedfollowers.size());
		
		int number = 0;
		for(ArrayList follow : orderedfollowers){
			number += follow.size();
		}
		System.out.println("Size of Followers: "+number);
		return orderedfollowers;
	}
	
	
	
	private static int[] getNumberOrder(ArrayList<Integer> numbers) {
		// TODO Auto-generated method stub
		int [] numberorder = new int [numbers.size()];
		
		for(int i = 0; i<numbers.size(); i++){
			numberorder[i] = numbers.indexOf(i);
		}
		
		return numberorder;
	}
	
}
