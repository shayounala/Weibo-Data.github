package dataprocessing;

import java.util.ArrayList;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.QueryOperators;

public class ExportDataToMongo {

	@SuppressWarnings("unchecked")
	public ArrayList<Integer> ExportUniqueUserIDs(DB db, String CollectionName) {
		ArrayList<Integer> UniqueUserIDsList = new ArrayList<Integer>();

		DBCollection UniqueUserIDsCollection = db.getCollection(CollectionName);
		UniqueUserIDsList = (ArrayList<Integer>) UniqueUserIDsCollection
				.distinct("ID", (new BasicDBObject("ID", new BasicDBObject(
						QueryOperators.EXISTS, true))));

		return UniqueUserIDsList;
	}

	@SuppressWarnings("unchecked")
	public void ExportNextUniqueIDFollowers(DB db, String CollectionName,
			int nextuniqueid) {

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
	public void ExportNextUniqueIDFriends(DB db, String CollectionName,
			int nextuniqueid) {

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
	public void ExportNextUniqueIDTweet(DB db, String CollectionName,
			int nextuniqueid) {

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
	public void ExportNextTweetIDRepost(DB db, String CollectionName,
			int nextuniqueid) {

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

	public void ExportUserFollowersID(DB db, String CollectionName, int userid,
			ArrayList<Integer> followers) {

		DBCollection UserInformationCollection = db
				.getCollection(CollectionName);
		BasicDBObject query = new BasicDBObject("ID", userid);
		BasicDBObject userinformationobject = (BasicDBObject) UserInformationCollection
				.findOne(query);
		if (userinformationobject.getBoolean("Followers ID")) {
			return;
		}

		userinformationobject.put("Followers ID", followers);
		UserInformationCollection.update(query, userinformationobject);
	}

	public void ExportUserFriendsID(DB db, String CollectionName, int userid,
			ArrayList<Integer> friends) {

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

	public void ExportUserTweetsID(DB db, String CollectionName, int userid,
			ArrayList<Integer> TweetsID) {

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
