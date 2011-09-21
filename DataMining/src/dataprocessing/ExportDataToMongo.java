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
		
		System.out.println("Export: Number of Unique User IDs: "+UniqueUserIDsCollection.find().count());

	}
	
	
	public void ExportUniqueTweetIDs(Processing processing,
			ArrayList<Long> UniqueTweetIDsList) {

		DB db = processing.getDB();
		String CollectionName = processing.getUniqueTweetIDs();
		DBCollection UniqueTweetIDsCollection = db.getCollection(CollectionName);

		for (int i = 0; i < UniqueTweetIDsList.size(); i++) {
			if(UniqueTweetIDsCollection.findOne(new BasicDBObject("ID",UniqueTweetIDsList.get(i))) == null){
				UniqueTweetIDsCollection.insert(new BasicDBObject("Number", i)
				.append("ID", UniqueTweetIDsList.get(i).intValue()));
			}

		}
		
		System.out.println("Export: Number of Unique Tweet IDs: "+UniqueTweetIDsCollection.find().count());

	}
	

	@SuppressWarnings("unchecked")
	public void ExportNextUniqueIDFollowers(Processing processing,
			int nextuniqueid) {

		DB db = processing.getDB();
		String CollectionName = processing.getNextIDs();
		DBCollection UserInformationCollection = db.getCollection(CollectionName);

		ArrayList<Integer> NextUniqueIDsList = (ArrayList<Integer>) UserInformationCollection
				.distinct(
						"Next ID for Mining followers ID",
						(new BasicDBObject("Next ID for Mining followers ID",
								new BasicDBObject(QueryOperators.EXISTS, true))));

		if (NextUniqueIDsList.size() != 1) {
			System.out
					.println("The format of Exported Next ID for Mining followers IDis wrong");
		} else {
			UserInformationCollection.findAndModify(new BasicDBObject(
					"Next ID for Mining followers ID", new BasicDBObject(
							QueryOperators.EXISTS, true)), new BasicDBObject(
					"Next ID for Mining followers ID", nextuniqueid));
		}
		
		System.out.println("Export: Next number of follower User IDs: "+nextuniqueid);

	}

	@SuppressWarnings("unchecked")
	public void ExportNextUniqueIDFriends(Processing processing,
			int nextuniqueid) {

		DB db = processing.getDB();
		String CollectionName = processing.getNextIDs();
		DBCollection UserInformationCollection = db.getCollection(CollectionName);

		ArrayList<Integer> NextUniqueIDsList = (ArrayList<Integer>) UserInformationCollection
				.distinct("Next ID for Mining friends ID", (new BasicDBObject(
						"Next ID for Mining friends ID", new BasicDBObject(
								QueryOperators.EXISTS, true))));

		if (NextUniqueIDsList.size() != 1) {
			System.out
					.println("The format of Exported Next ID for Mining friends ID is wrong");
		} else {
			UserInformationCollection.findAndModify(new BasicDBObject(
					"Next ID for Mining friends ID", new BasicDBObject(
							QueryOperators.EXISTS, true)), new BasicDBObject(
					"Next ID for Mining friends ID", nextuniqueid));
		}
		
		System.out.println("Export: Next number of friends User IDs:"+ nextuniqueid);

	}

	@SuppressWarnings("unchecked")
	public void ExportNextUniqueIDTweet(Processing processing, int nextuniqueid) {

		DB db = processing.getDB();
		String CollectionName = processing.getNextIDs();
		DBCollection UserInformationCollection = db.getCollection(CollectionName);

		ArrayList<Integer> NextUniqueIDsList = (ArrayList<Integer>) UserInformationCollection
				.distinct("Next ID for Mining Tweets", (new BasicDBObject(
						"Next ID for Mining Tweets", new BasicDBObject(
								QueryOperators.EXISTS, true))));

		if (NextUniqueIDsList.size() != 1) {
			System.out
					.println("The format of Exported Next ID for Mining Tweets is wrong");
		} else {
			UserInformationCollection.findAndModify(new BasicDBObject(
					"Next ID for Mining Tweets", new BasicDBObject(
							QueryOperators.EXISTS, true)), new BasicDBObject(
					"Next ID for Mining Tweets", nextuniqueid));
		}
		
		System.out.println("Export: Next number of tweets User IDs: "+nextuniqueid);

	}

	@SuppressWarnings("unchecked")
	public void ExportNextTweetIDRepost(Processing processing, int nextuniqueid) {

		DB db = processing.getDB();
		String CollectionName = processing.getNextIDs();
		DBCollection UniqueTweetIDsCollection = db.getCollection(CollectionName);

		ArrayList<Integer> NextUniqueIDsList = (ArrayList<Integer>) UniqueTweetIDsCollection
				.distinct(
						"Next Tweet ID for Mining Reposts",
						(new BasicDBObject("Next Tweet ID for Mining Reposts",
								new BasicDBObject(QueryOperators.EXISTS, true))));

		if (NextUniqueIDsList.size() != 1) {
			System.out
					.println("The format of Exported Next Tweet ID for Mining Reposts is wrong");
		} else {
			UniqueTweetIDsCollection.findAndModify(new BasicDBObject(
					"Next Tweet ID for Mining Reposts", new BasicDBObject(
							QueryOperators.EXISTS, true)), new BasicDBObject(
					"Next Tweet ID for Mining Reposts", nextuniqueid));
		}
		
		System.out.println("Export: Next number of reposts Tweet IDs:"+nextuniqueid);

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
		
		
		System.out.println("Export: Number of followers User IDs: "+UserInformationCollection.find(new BasicDBObject("Followers ID", new BasicDBObject(QueryOperators.EXISTS,true))).count());
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
		
		System.out.println("Export: Number of friends User IDs: "+UserInformationCollection.find(new BasicDBObject("Friends ID", new BasicDBObject(QueryOperators.EXISTS,true))).count());
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
		
		System.out.println("Export: Number of tweets User IDs: "+UserInformationCollection.find(new BasicDBObject("Tweets ID", new BasicDBObject(QueryOperators.EXISTS,true))).count());
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
