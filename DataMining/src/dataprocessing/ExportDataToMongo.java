package dataprocessing;

import java.util.ArrayList;
import java.util.Date;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.QueryOperators;

public class ExportDataToMongo {

	@SuppressWarnings("unchecked")
	public ArrayList<Long> ExportUniqueUserIDs(Processing processing,
			ArrayList<Long> UniqueUserIDsList, ArrayList<Long> Followers) {

		DB db = processing.getDB();
		String CollectionName = processing.getUniqueUserIDs();
		DBCollection UniqueUserIDsCollection = db.getCollection(CollectionName);
		System.out.println(new Date(System.currentTimeMillis()));
		for(int i=0;i<Followers.size();i++){
			
			for(int j=0;j<UniqueUserIDsList.size();j++){
				if(UniqueUserIDsList.get(j).longValue()==Followers.get(i).longValue()){
					UniqueUserIDsList.add(Followers.get(i));
				}
			}
		}
		System.out.println(new Date(System.currentTimeMillis()));
		BasicDBObject query = new BasicDBObject("User ID", new BasicDBObject(QueryOperators.EXISTS,true));
		BasicDBObject updatedresults = new BasicDBObject("User ID", UniqueUserIDsList);
		UniqueUserIDsCollection.update(query, updatedresults);
		System.out.println(new Date(System.currentTimeMillis()));
		System.out.println("Export: Number of Unique User IDs: "+UniqueUserIDsList.size());

		return UniqueUserIDsList;

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

		BasicDBObject query = new BasicDBObject("User ID", userid);
		BasicDBObject userinformationobject = (BasicDBObject) UserInformationCollection
				.findOne(query);
		if (userinformationobject == null) {
			userinformationobject = new BasicDBObject("User ID", userid);
			UserInformationCollection.insert(userinformationobject);
		}
		if (userinformationobject.get("Followers ID")!=null) {
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

		BasicDBObject query = new BasicDBObject("User ID", userid);
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
	
	
	public void ExportUniqueTweetsID(Processing processing,
			ArrayList<Long> TweetsID) {

		DB db = processing.getDB();
		String CollectionName = processing.getUniqueTweetIDs();
		DBCollection UniqueTweetIDsCollection = db
				.getCollection(CollectionName);

		for (int i = 0; i < TweetsID.size(); i++) {

			UniqueTweetIDsCollection.insert(new BasicDBObject("Tweet Number", i)
			.append("Tweet ID", TweetsID.get(i).intValue()));

		}
		
		System.out.println("Export: Number of tweets IDs: "+UniqueTweetIDsCollection.find(new BasicDBObject("Tweet ID", new BasicDBObject(QueryOperators.EXISTS,true))).count());
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

		BasicDBObject query = new BasicDBObject("Tweet ID", tweetid);
		BasicDBObject tweetinformationobject = (BasicDBObject) UserInformationCollection
				.findOne(query);
		
		if (tweetinformationobject == null) {
			tweetinformationobject = new BasicDBObject("Tweet ID", tweetid);
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

		BasicDBObject query = new BasicDBObject("Tweet ID", userid);
		BasicDBObject tweetinformationobject = (BasicDBObject) UserInformationCollection
				.findOne(query);
		if (tweetinformationobject.getBoolean("Repost")) {
			return;
		}

		tweetinformationobject.put("Repost", reposts);
		UserInformationCollection.update(query, tweetinformationobject);
	}

}
