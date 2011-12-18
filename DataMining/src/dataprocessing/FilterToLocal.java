package dataprocessing;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;

import com.mongodb.BasicDBObject;
import com.mongodb.Bytes;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.QueryOperators;

public class FilterToLocal {

	@SuppressWarnings("unused")
	public static void main(String[] args) {
		try {
			Mongo mongo = new Mongo("10.3.4.84", 27017);
			mongo.getDB("mydb").authenticate("cssc", new char[] { '1' });
			mongo.getDB("filterdb").authenticate("cssc", new char[] { '1' });
			mongo.getDB("filtereddb").authenticate("cssc", new char[] { '1' });
			mongo.getDB("weibodb").authenticate("cssc", new char[] { '1' });

			DB dbOrigin = mongo.getDB("mydb");
			DB dbFilter = mongo.getDB("filterdb");
			DB dbFiltered = mongo.getDB("filtereddb");
			DB weibodb = mongo.getDB("weibodb");
			dbFilter.dropDatabase();
			dbFiltered.dropDatabase();
			// dbFiltered.getCollection("UserInformation").drop();

			// FromServerToLocal(dbOrigin, "UserInformation", dbFilter,
			// "UserInformation");
			// FilterFollowersForLocal(dbFilter, "UserInformation");
			// FilterUsersForLocal(dbFiltered, "UserInformation");
			// OutputFilterdbInformation(dbFilter, dbFiltered,
			// "UserInformation1");
			testDataBase(dbFiltered, weibodb);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MongoException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println(new Date());
		}

	}

	private static void testDataBase(DB dbFiltered, DB weibodb) {
		// TODO Auto-generated method stub
		// System.out.println(dbFiltered.getCollection("UserInformation").count());
		/*
		 * weibodb.getCollection("RegularUserInformation").drop(); DBCollection
		 * collection = dbFiltered.getCollection("UserInformation1");
		 * DBCollection collection1 =
		 * weibodb.getCollection("FilterUserInformation");
		 * 
		 * DBCursor cursor = collection.find();
		 * 
		 * for(int i=0;cursor.hasNext();i++){ collection1.insert(cursor.next());
		 * System.out.println(i); }
		 */

		System.out.println(weibodb.getCollection("UserInformation").count());
		System.out.println(weibodb.getCollection("FilterUserInformation")
				.count());

	}

	@SuppressWarnings({ "unchecked", "unused" })
	private static void OutputFilterdbInformation(DB dbFilter, DB dbFiltered,
			String CollectionName) {
		// TODO Auto-generated method stub
		System.out.println(dbFilter.getCollection("UserInformation").count());
		System.out.println(dbFilter.getCollection("UserInformation1").count());
		System.out.println(dbFiltered.getCollection("UserInformation").count());
		DBCollection collection = dbFilter.getCollection(CollectionName);
		DBCollection collection1 = dbFiltered.getCollection("UserInformation1");
		DBCursor cursor = collection1.find(
				new BasicDBObject("User IDs", new BasicDBObject(
						QueryOperators.EXISTS, true))).limit(5200);
		int users = 0, followers = 0;
		for (int i = 0; i < cursor.size(); i++) {
			BasicDBObject object = (BasicDBObject) cursor.next();
			ArrayList<Long> User = (ArrayList<Long>) object.get("User IDs");
			ArrayList<Long> Follower = (ArrayList<Long>) object
					.get("Followers IDs");

			System.out.println(object.get("Number") + " " + User.size());

			/*
			 * BasicDBObject information = new BasicDBObject();
			 * information.append("Number", object.get("Number"));
			 * information.append("User IDs", User);
			 * information.append("Followers IDs", Follower);
			 * collection1.insert(information);
			 */
			users = users + User.size();
			followers = followers + Follower.size();
			User.clear();
			Follower.clear();
		}
		System.out.println(users + "   " + followers);
	}

	@SuppressWarnings({ "unchecked", "unused" })
	private static void FilterUsersForLocal(DB dbFiltered, String CollectionName) {
		// TODO Auto-generated method stub
		DBCollection collection = dbFiltered.getCollection(CollectionName);
		dbFiltered.getCollection("UserInformation1").drop();
		DBCollection collection1 = dbFiltered.getCollection("UserInformation1");
		DBCursor cursor = collection.find(new BasicDBObject("User IDs",
				new BasicDBObject(QueryOperators.EXISTS, true)));
		cursor.setOptions(Bytes.QUERYOPTION_NOTIMEOUT);

		ArrayList<Long> UniqueUsers = new ArrayList<Long>();
		ArrayList<Long> followers = new ArrayList<Long>();
		ArrayList<Long> newUniqueUsers = new ArrayList<Long>();
		ArrayList<Long> newfollowers = new ArrayList<Long>();
		for (int i = 0; i < cursor.count(); i++) {
			BasicDBObject object = (BasicDBObject) cursor.next();
			int number = object.getInt("Number");
			UniqueUsers = (ArrayList<Long>) object.get("User IDs");
			followers = (ArrayList<Long>) object.get("Followers IDs");
			System.out.println(UniqueUsers.size());
			System.out.println(followers.size());

			int flagforUniqueUsers = 0;
			ArrayList<Long> follower = new ArrayList<Long>();
			for (int j = 0; j < followers.size(); j++) {
				long followerid = followers.get(j);
				follower.add(followers.get(j));

				if (followerid == -1) {
					if (follower.size() >= 2) {
						newfollowers.addAll(follower);
						newUniqueUsers.add(UniqueUsers.get(flagforUniqueUsers));
					}
					follower.clear();
					flagforUniqueUsers++;
				}

			}

			System.out.println(newUniqueUsers.size());
			System.out.println(newfollowers.size());
			BasicDBObject information = new BasicDBObject();
			information.append("Number", number);
			information.append("User IDs", newUniqueUsers);
			information.append("Followers IDs", newfollowers);

			collection1.insert(information);
			object.clear();
			UniqueUsers.clear();
			followers.clear();
			newUniqueUsers.clear();
			newfollowers.clear();

			System.out.println("Iretator of Filter Users: " + i);
		}
	}

	@SuppressWarnings({ "unchecked", "unused" })
	private static void FilterFollowersForLocal(DB dbFilter,
			String CollectionName) {
		// TODO Auto-generated method stub
		DBCollection collection = dbFilter.getCollection(CollectionName);
		DBCursor cursor = collection.find(new BasicDBObject("Followers IDs",
				new BasicDBObject(QueryOperators.EXISTS, true)));
		cursor.setOptions(Bytes.QUERYOPTION_NOTIMEOUT);

		ArrayList<Long> UniqueUsers = new ArrayList<Long>();
		for (int i = 0; i < cursor.count(); i++) {
			ArrayList<Long> UniqueUser = (ArrayList<Long>) cursor.next().get(
					"User IDs");
			UniqueUsers.addAll(UniqueUser);

		}
		System.out.println(cursor.count());
		cursor = collection.find(new BasicDBObject("Followers IDs",
				new BasicDBObject(QueryOperators.EXISTS, true)));
		ArrayList<Long> followers = new ArrayList<Long>();
		for (int i = 0; i < cursor.count(); i++) {
			BasicDBObject object = (BasicDBObject) cursor.next();
			followers = (ArrayList<Long>) object.get("Followers IDs");
			for (int j = 0; j < followers.size(); j++) {
				long followerid = followers.get(j).longValue();
				boolean contain = false;

				if (followerid != -1L) {
					for (int k = 0; k < UniqueUsers.size(); k++) {
						if (followerid == UniqueUsers.get(k)) {
							contain = true;
							break;
						}
					}
				} else {
					contain = true;
				}

				if (!contain) {
					followers.remove(followerid);
					j--;
				}
			}

			BasicDBObject information = new BasicDBObject();
			information.append("Number", i + 1);
			information.append("User IDs", UniqueUsers);
			information.append("Followers IDs", followers);

			collection.update(new BasicDBObject("Number", i + 1), information);
			object.clear();
			followers.clear();
			System.out.println("Iretator of Filter Followers: " + i);
		}

	}

	private static void WriteToLocal(DB dbLocal, String LocalCollectionName,
			ArrayList<Long> userids, ArrayList<Long> followerids) {
		// TODO Auto-generated method stub
		DBCollection collection = dbLocal.getCollection(LocalCollectionName);

		BasicDBObject information = new BasicDBObject();
		information.append("Number", collection.count() + 1);
		information.append("User IDs", userids);
		information.append("Followers IDs", followerids);

		collection.insert(information);
		System.out.println("Count of UserInformation in Filterdb: "
				+ collection.count());
		System.out.println(System.currentTimeMillis());
		// System.exit(0);
	}

	@SuppressWarnings({ "unchecked", "unused" })
	private static void FromServerToLocal(DB dbServer,
			String ServerCollectionName, DB dbLocal, String LocalCollectionName) {
		// TODO Auto-generated method stub
		DBCollection collection = dbServer.getCollection(ServerCollectionName);
		DBCursor cursor = collection.find(new BasicDBObject("User ID",
				new BasicDBObject(QueryOperators.EXISTS, true)));

		cursor.setOptions(Bytes.QUERYOPTION_NOTIMEOUT);
		System.out.println(cursor.count());

		int number = 0;
		ArrayList<Long> followers = new ArrayList<Long>();
		ArrayList<Long> userids = new ArrayList<Long>();
		ArrayList<Long> followerids = new ArrayList<Long>();
		for (int i = 0; i < cursor.size(); i++) {
			BasicDBObject information = (BasicDBObject) cursor.next();
			followers = (ArrayList<Long>) information.get("Followers ID");
			Long userid = (Long) information.get("User ID");
			if (followers.size() > 20) {
				number++;
				userids.add(userid);
				followerids.addAll(followers);
				followerids.add(-1L);

			}

			if (followerids.size() >= 10000 || i == cursor.size() - 1) {
				WriteToLocal(dbLocal, LocalCollectionName, userids, followerids);
				userids.clear();
				followerids.clear();
			}
			followers.clear();
			information.clear();
			// System.out.println("Iretator: "+i+" Number: "+number);
		}

	}

}
