package dataprocessing;

import java.util.ArrayList;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.QueryOperators;

public class ExportDataToMongo {

	@SuppressWarnings({ "unchecked" })
	public void ExportUniqueUserIDs(Processing processing,
			ArrayList<Integer> UniqueUserIDsList) {

		DB db = processing.getDB();
		String CollectionName = processing.getUniqueUserIDs();
		DBCollection UniqueUserIDsCollection = db.getCollection(CollectionName);

		int number = UniqueUserIDsCollection.find().count();
		ArrayList<Integer> lastid = (ArrayList<Integer>) UniqueUserIDsCollection
				.distinct("ID", new BasicDBObject("Number", number - 1));
		if (lastid.size() != 1) {
			System.out
					.println("There exists more than one DBObject for Number = "
							+ (number - 1) + " in " + CollectionName);
		} else if (lastid.get(0) != UniqueUserIDsList.get(number - 1)) {
			System.out
					.println("New exported unique user data doesn't equals the old data");
		}

		for (int i = number; i < UniqueUserIDsList.size(); i++) {
			UniqueUserIDsCollection.insert(new BasicDBObject("Number", i)
					.append("ID", UniqueUserIDsList.get(i).intValue()));
		}

	}

	@SuppressWarnings("unchecked")
	public void ExportNextUniqueIDFollowers(Processing processing,
			int nextuniqueid) {

		DB db = processing.getDB();
		String CollectionName = processing.getNextIDs();
		DBCollection UniqueUserIDsCollection = db.getCollection(CollectionName);

		ArrayList<Integer> NextUniqueIDsList = (ArrayList<Integer>) UniqueUserIDsCollection
				.distinct(
						"Next ID for Mining followers ID",
						(new BasicDBObject("Next ID for Mining followers ID",
								new BasicDBObject(QueryOperators.EXISTS, true))));

		if (NextUniqueIDsList.size() != 1) {
			System.out
					.println("The format of Exported Next ID for Mining followers IDis wrong");
		} else {
			UniqueUserIDsCollection.findAndModify(new BasicDBObject(
					"Next ID for Mining followers ID", new BasicDBObject(
							QueryOperators.EXISTS, true)), new BasicDBObject(
					"Next ID for Mining followers ID", nextuniqueid));
		}

	}

	@SuppressWarnings("unchecked")
	public void ExportNextUniqueIDFriends(Processing processing,
			int nextuniqueid) {

		DB db = processing.getDB();
		String CollectionName = processing.getNextIDs();
		DBCollection UniqueUserIDsCollection = db.getCollection(CollectionName);

		ArrayList<Integer> NextUniqueIDsList = (ArrayList<Integer>) UniqueUserIDsCollection
				.distinct("Next ID for Mining friends ID", (new BasicDBObject(
						"Next ID for Mining friends ID", new BasicDBObject(
								QueryOperators.EXISTS, true))));

		if (NextUniqueIDsList.size() != 1) {
			System.out
					.println("The format of Exported Next ID for Mining friends ID is wrong");
		} else {
			UniqueUserIDsCollection.findAndModify(new BasicDBObject(
					"Next ID for Mining friends ID", new BasicDBObject(
							QueryOperators.EXISTS, true)), new BasicDBObject(
					"Next ID for Mining friends ID", nextuniqueid));
		}

	}

	@SuppressWarnings("unchecked")
	public void ExportNextUniqueIDTweet(Processing processing, int nextuniqueid) {

		DB db = processing.getDB();
		String CollectionName = processing.getNextIDs();
		DBCollection UniqueUserIDsCollection = db.getCollection(CollectionName);

		ArrayList<Integer> NextUniqueIDsList = (ArrayList<Integer>) UniqueUserIDsCollection
				.distinct("Next ID for Mining Tweets", (new BasicDBObject(
						"Next ID for Mining Tweets", new BasicDBObject(
								QueryOperators.EXISTS, true))));

		if (NextUniqueIDsList.size() != 1) {
			System.out
					.println("The format of Exported Next ID for Mining Tweets is wrong");
		} else {
			UniqueUserIDsCollection.findAndModify(new BasicDBObject(
					"Next ID for Mining Tweets", new BasicDBObject(
							QueryOperators.EXISTS, true)), new BasicDBObject(
					"Next ID for Mining Tweets", nextuniqueid));
		}

	}

	@SuppressWarnings("unchecked")
	public void ExportNextTweetIDRepost(Processing processing, int nextuniqueid) {

		DB db = processing.getDB();
		String CollectionName = processing.getNextIDs();
		DBCollection UniqueUserIDsCollection = db.getCollection(CollectionName);

		ArrayList<Integer> NextUniqueIDsList = (ArrayList<Integer>) UniqueUserIDsCollection
				.distinct(
						"Next Tweet ID for Mining Reposts",
						(new BasicDBObject("Next Tweet ID for Mining Reposts",
								new BasicDBObject(QueryOperators.EXISTS, true))));

		if (NextUniqueIDsList.size() != 1) {
			System.out
					.println("The format of Exported Next Tweet ID for Mining Reposts is wrong");
		} else {
			UniqueUserIDsCollection.findAndModify(new BasicDBObject(
					"Next Tweet ID for Mining Reposts", new BasicDBObject(
							QueryOperators.EXISTS, true)), new BasicDBObject(
					"Next Tweet ID for Mining Reposts", nextuniqueid));
		}

	}

	public void ExportUserFollowersID(Processing processing, int userid,
			ArrayList<Integer> followers) {

		DB db = processing.getDB();
		String CollectionName = processing.getUserinformation();
		DBCollection UserInformationCollection = db
				.getCollection(CollectionName);

		BasicDBObject query = new BasicDBObject("ID", userid);
		BasicDBObject userinformationobject = (BasicDBObject) UserInformationCollection
				.findOne(query);
		if (userinformationobject == null) {
			userinformationobject = new BasicDBObject("ID", userid);
		}
		if (userinformationobject.getBoolean("Followers ID")) {
			return;
		}

		userinformationobject.put("Followers ID", followers);
		UserInformationCollection.update(query, userinformationobject);
	}

	public void ExportUserFriendsID(Processing processing, int userid,
			ArrayList<Integer> friends) {

		DB db = processing.getDB();
		String CollectionName = processing.getUserinformation();
		DBCollection UserInformationCollection = db
				.getCollection(CollectionName);

		BasicDBObject query = new BasicDBObject("ID", userid);
		BasicDBObject userinformationobject = (BasicDBObject) UserInformationCollection
				.findOne(query);
		if (userinformationobject.getBoolean("Friends ID")) {
			return;
		}

		userinformationobject.put("Friends ID", friends);
		UserInformationCollection.update(query, userinformationobject);
	}

	public void ExportUserTweetsID(Processing processing, int userid,
			ArrayList<Integer> TweetsID) {

		DB db = processing.getDB();
		String CollectionName = processing.getUserinformation();
		DBCollection UserInformationCollection = db
				.getCollection(CollectionName);

		BasicDBObject query = new BasicDBObject("ID", userid);
		BasicDBObject userinformationobject = (BasicDBObject) UserInformationCollection
				.findOne(query);
		if (userinformationobject.getBoolean("Tweets ID")) {
			return;
		}

		userinformationobject.put("Tweets ID", TweetsID);
		UserInformationCollection.update(query, userinformationobject);
	}

}
