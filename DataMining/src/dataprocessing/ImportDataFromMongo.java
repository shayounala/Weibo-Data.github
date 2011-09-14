package dataprocessing;

import java.util.ArrayList;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.QueryOperators;

public class ImportDataFromMongo {

	@SuppressWarnings("unchecked")
	public ArrayList<Integer> importUniqueUserIDs(DB db, String CollectionName) {
		ArrayList<Integer> UniqueUserIDsList = new ArrayList<Integer>();

		DBCollection UniqueUserIDsCollection = db.getCollection(CollectionName);
		UniqueUserIDsList = (ArrayList<Integer>) UniqueUserIDsCollection
				.distinct("ID", (new BasicDBObject("ID", new BasicDBObject(
						QueryOperators.EXISTS, true))));

		return UniqueUserIDsList;
	}

	@SuppressWarnings("unchecked")
	public int importNextUniqueIDFollowers(DB db, String CollectionName) {
		int nextuniqueid = 0;

		DBCollection UniqueUserIDsCollection = db.getCollection(CollectionName);
		ArrayList<Integer> NextUniqueIDsList = (ArrayList<Integer>) UniqueUserIDsCollection
				.distinct(
						"Next ID for Mining followers ID",
						(new BasicDBObject("Next ID for Mining followers ID",
								new BasicDBObject(QueryOperators.EXISTS, true))));

		if (NextUniqueIDsList.size() != 1) {
			System.out.println("The format of imported Next ID for Mining followers IDis wrong");
		} else {
			nextuniqueid = NextUniqueIDsList.get(0);
		}

		return nextuniqueid;
	}

	@SuppressWarnings("unchecked")
	public int importNextUniqueIDFriends(DB db, String CollectionName) {
		int nextuniqueid = 0;

		DBCollection UniqueUserIDsCollection = db.getCollection(CollectionName);
		ArrayList<Integer> NextUniqueIDsList = (ArrayList<Integer>) UniqueUserIDsCollection
				.distinct("Next ID for Mining friends ID", (new BasicDBObject(
						"Next ID for Mining friends ID", new BasicDBObject(
								QueryOperators.EXISTS, true))));

		if (NextUniqueIDsList.size() != 1) {
			System.out.println("The format of imported Next ID for Mining friends ID is wrong");
		} else {
			nextuniqueid = NextUniqueIDsList.get(0);
		}

		return nextuniqueid;
	}

	@SuppressWarnings("unchecked")
	public int importNextUniqueIDTweet(DB db, String CollectionName) {
		int nextuniqueid = 0;

		DBCollection UniqueUserIDsCollection = db.getCollection(CollectionName);
		ArrayList<Integer> NextUniqueIDsList = (ArrayList<Integer>) UniqueUserIDsCollection
				.distinct(
						"Next ID for Mining Tweets",
						(new BasicDBObject("Next ID for Mining Tweets",
								new BasicDBObject(QueryOperators.EXISTS, true))));

		if (NextUniqueIDsList.size() != 1) {
			System.out.println("The format of imported Next ID for Mining Tweets is wrong");
		} else {
			nextuniqueid = NextUniqueIDsList.get(0);
		}

		return nextuniqueid;
	}

	@SuppressWarnings("unchecked")
	public int importNextTweetIDRepost(DB db, String CollectionName) {
		int nextuniqueid = 0;

		DBCollection UniqueUserIDsCollection = db.getCollection(CollectionName);
		ArrayList<Integer> NextUniqueIDsList = (ArrayList<Integer>) UniqueUserIDsCollection
				.distinct(
						"Next Tweet ID for Mining Reposts",
						(new BasicDBObject("Next Tweet ID for Mining Reposts",
								new BasicDBObject(QueryOperators.EXISTS, true))));

		if (NextUniqueIDsList.size() != 1) {
			System.out.println("The format of imported Next Tweet ID for Mining Reposts is wrong");
		} else {
			nextuniqueid = NextUniqueIDsList.get(0);
		}

		return nextuniqueid;
	}

}
