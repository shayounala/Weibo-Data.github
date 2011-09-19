package dataprocessing;

import java.util.ArrayList;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.QueryOperators;

public class ExportDataToMongo {

	public void ExportUniqueUserIDs(Processing processing,
			ArrayList<Long> UniqueUserIDsList) {

		DB db = processing.getDB();
		String CollectionName = processing.getUniqueUserIDs();
		DBCollection UniqueUserIDsCollection = db.getCollection(CollectionName);

		for (int i = 0; i < UniqueUserIDsList.size(); i++) {
			if(UniqueUserIDsCollection.findOne(new BasicDBObject("ID",UniqueUserIDsList.get(i))) == null){
				UniqueUserIDsCollection.insert(new BasicDBObject("Number", i)
				.append("ID", UniqueUserIDsList.get(i).intValue()));
			}

		}

	}
	
	
	public void ExportUniqueTweetIDs(Processing processing,
			ArrayList<Long> UniqueTweetIDsList) {

		DB db = processing.getDB();
		String CollectionName = processing.getUniqueTweetIDs();
		DBCollection UniqueUserIDsCollection = db.getCollection(CollectionName);

		for (int i = 0; i < UniqueTweetIDsList.size(); i++) {
			if(UniqueUserIDsCollection.findOne(new BasicDBObject("ID",UniqueTweetIDsList.get(i))) == null){
				UniqueUserIDsCollection.insert(new BasicDBObject("Number", i)
				.append("ID", UniqueTweetIDsList.get(i).intValue()));
			}

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

	public void ExportUserFollowersID(Processing processing, long userid,
			ArrayList<Long> followers) {

		DB db = processing.getDB();
		String CollectionName = processing.getUserInformation();
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

	public void ExportUserFriendsID(Processing processing, long userid,
			ArrayList<Long> friends) {

		DB db = processing.getDB();
		String CollectionName = processing.getUserInformation();
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

	public void ExportUserTweetsID(Processing processing, long userid,
			ArrayList<Long> TweetsID) {

		DB db = processing.getDB();
		String CollectionName = processing.getUserInformation();
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

	public void ExportAccountInformation(Processing processing, String weiboAccount,
			String weiboPassword, String weiboToken,
			String weiboTokenSecret) {
		// TODO Auto-generated method stub
		DB db = processing.getDB();
		String CollectionName = processing.getAccountInformation();
		DBCollection AccountInformationCollection = db
				.getCollection(CollectionName);
		

			BasicDBObject accountinfo = new BasicDBObject();
			accountinfo.append("weiboAccount", weiboAccount);
			System.out.println(weiboAccount);
			accountinfo.append("weiboPassword", weiboPassword);
			System.out.println(weiboPassword);
			accountinfo.append("weiboToken", weiboToken);
			System.out.println(weiboToken);
			accountinfo.append("weiboTokenSecret", weiboTokenSecret);
			System.out.println(weiboTokenSecret);
			AccountInformationCollection.insert(accountinfo);
		
	}
	
	public void ExportUserTweetInformationID(Processing processing, long tweetid,
			ArrayList<Long> tweets) {

		DB db = processing.getDB();
		String CollectionName = processing.getTweetInformation();
		DBCollection UserInformationCollection = db
				.getCollection(CollectionName);

		BasicDBObject query = new BasicDBObject("ID", tweetid);
		BasicDBObject tweetinformationobject = (BasicDBObject) UserInformationCollection
				.findOne(query);
		
		if (tweetinformationobject == null) {
			tweetinformationobject = new BasicDBObject("ID", tweetid);
		}
		
		if (tweetinformationobject.getBoolean("Tweet")) {
			return;
		}

		tweetinformationobject.put("Tweet", tweets);
		UserInformationCollection.update(query, tweetinformationobject);
	}
	
	public void ExportTweetReposts(Processing processing, long userid,
			ArrayList<Long> reposts) {

		DB db = processing.getDB();
		String CollectionName = processing.getTweetInformation();
		DBCollection UserInformationCollection = db
				.getCollection(CollectionName);

		BasicDBObject query = new BasicDBObject("ID", userid);
		BasicDBObject tweetinformationobject = (BasicDBObject) UserInformationCollection
				.findOne(query);
		if (tweetinformationobject.getBoolean("Repost")) {
			return;
		}

		tweetinformationobject.put("Repost", reposts);
		UserInformationCollection.update(query, tweetinformationobject);
	}

}
