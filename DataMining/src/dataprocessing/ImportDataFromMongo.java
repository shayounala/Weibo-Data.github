package dataprocessing;

import java.util.ArrayList;
import java.util.Collection;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.QueryOperators;

public class ImportDataFromMongo {

	/**
	 * @param processing
	 * @return the arraylist of all the unique users ids
	 */
	@SuppressWarnings("unchecked")
	public ArrayList<Long> importUniqueUserIDs(Processing processing) {

		ArrayList<Long> UniqueUserIDsList = new ArrayList<Long>();
		DB db = processing.getDB();
		String CollectionName = processing.getUniqueUserIDs();

		DBCollection UniqueUserIDsCollection = db.getCollection(CollectionName);
		DBCursor cursor = UniqueUserIDsCollection.find(new BasicDBObject(
				"User ID", new BasicDBObject(QueryOperators.EXISTS, true)));
		for (int i = 0; i < cursor.count(); i++) {
			UniqueUserIDsList.addAll((Collection<? extends Long>) cursor.next()
					.get("User ID"));
		}

		System.out.println("Import: Number of Unique User IDs: "
				+ UniqueUserIDsList.size());
		return UniqueUserIDsList;
	}

	/**
	 * @param processing
	 * @return the next id for mining followers
	 */
	@SuppressWarnings("unchecked")
	public int importNextUniqueIDFollowers(Processing processing) {

		int nextuniqueid = 0;
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
					.println("The format of imported Next ID for Mining followers ID is wrong");
		} else {
			nextuniqueid = NextUniqueIDsList.get(0);
		}

		System.out.println("Import: Next number of follower User IDs: "
				+ nextuniqueid);
		return nextuniqueid;
	}

	/**
	 * @param processing
	 * @return the next id for mining friends
	 */
	@SuppressWarnings("unchecked")
	public int importNextUniqueIDFriends(Processing processing) {

		int nextuniqueid = 0;
		DB db = processing.getDB();
		String CollectionName = processing.getNextIDs();

		DBCollection UniqueUserIDsCollection = db.getCollection(CollectionName);
		ArrayList<Integer> NextUniqueIDsList = (ArrayList<Integer>) UniqueUserIDsCollection
				.distinct("Next ID for Mining friends ID", (new BasicDBObject(
						"Next ID for Mining friends ID", new BasicDBObject(
								QueryOperators.EXISTS, true))));

		if (NextUniqueIDsList.size() != 1) {
			System.out
					.println("The format of imported Next ID for Mining friends ID is wrong");
		} else {
			nextuniqueid = NextUniqueIDsList.get(0);
		}

		System.out.println("Import: Next number of friends User IDs: "
				+ nextuniqueid);
		return nextuniqueid;
	}

	/**
	 * @param processing
	 * @return the next id for mining tweets
	 */
	@SuppressWarnings("unchecked")
	public int importNextUniqueIDTweet(Processing processing) {

		int nextuniqueid = 0;
		DB db = processing.getDB();
		String CollectionName = processing.getNextIDs();

		DBCollection UniqueUserIDsCollection = db.getCollection(CollectionName);
		ArrayList<Integer> NextUniqueIDsList = (ArrayList<Integer>) UniqueUserIDsCollection
				.distinct("Next ID for Mining Tweets", (new BasicDBObject(
						"Next ID for Mining Tweets", new BasicDBObject(
								QueryOperators.EXISTS, true))));

		if (NextUniqueIDsList.size() != 1) {
			System.out
					.println("The format of imported Next ID for Mining Tweets is wrong");
			System.out.println(NextUniqueIDsList);
		} else {
			nextuniqueid = NextUniqueIDsList.get(0);
		}

		System.out.println("Import: Next number of tweet User IDs: "
				+ nextuniqueid);
		return nextuniqueid;
	}

	/**
	 * @param processing
	 * @return the next tweet id for mining repost
	 */
	@SuppressWarnings("unchecked")
	public int importNextTweetIDRepost(Processing processing) {

		int nextuniqueid = 0;
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
					.println("The format of imported Next Tweet ID for Mining Reposts is wrong");
		} else {
			nextuniqueid = NextUniqueIDsList.get(0);
		}

		System.out.println("Import: Next number of repost Tweet IDs: "
				+ nextuniqueid);
		return nextuniqueid;
	}

	/**
	 * @param processing
	 * @return all the accounts information in the database, the structure of
	 *         the results follows: all the Account Name, all the Account
	 *         Password, all the Account Token, all the Account Secret Token
	 */
	@SuppressWarnings("unchecked")
	public ArrayList<String> importAccountInfomation(Processing processing) {
		// TODO Auto-generated method stub
		ArrayList<String> AccountInfo = new ArrayList<String>();
		DB db = processing.getDB();
		String CollectionName = processing.getAccountInformation();
		DBCollection AccountInfoCollection = db.getCollection(CollectionName);

		AccountInfo.addAll(AccountInfoCollection.distinct("weiboAccount"));
		AccountInfo.addAll(AccountInfoCollection.distinct("weiboPassword"));
		AccountInfo.addAll(AccountInfoCollection.distinct("weiboToken"));
		AccountInfo.addAll(AccountInfoCollection.distinct("weiboTokenSecret"));

		return AccountInfo;

	}

	/**
	 * @param processing
	 * @return the arraylist of all the unique tweet ids
	 */
	@SuppressWarnings("unchecked")
	public ArrayList<Long> importUniqueTweetIDs(Processing processing) {

		ArrayList<Long> UniqueTweetIDsList = new ArrayList<Long>();
		DB db = processing.getDB();
		String CollectionName = processing.getUniqueTweetIDs();

		DBCollection UniqueUserIDsCollection = db.getCollection(CollectionName);
		UniqueTweetIDsList = (ArrayList<Long>) UniqueUserIDsCollection
				.distinct("Tweet ID", (new BasicDBObject("Tweet ID",
						new BasicDBObject(QueryOperators.EXISTS, true))));

		System.out.println("Import: Number of Unique Tweet IDs: "
				+ UniqueTweetIDsList.size());
		return UniqueTweetIDsList;
	}

	/**
	 * @param processing
	 * @param weiboNumberMax
	 * @return return the accounts information (number of the accounts is
	 *         weiboNumberMax) from the database, the structure of the results
	 *         follows: weiboNumberMax of Account Name, weiboNumberMax of
	 *         Account Password, weiboNumberMax of Account Token, weiboNumberMax
	 *         of Account SecretToken
	 */
	@SuppressWarnings("unchecked")
	public ArrayList<String> importAccountInfomation(Processing processing,
			int weiboNumberMax) {
		// TODO Auto-generated method stub
		ArrayList<String> AccountInfo = new ArrayList<String>();
		DB db = processing.getDB();
		String CollectionName = processing.getAccountInformation();
		DBCollection AccountInfoCollection = db.getCollection(CollectionName);

		ArrayList<String> weiboaccount = (ArrayList<String>) AccountInfoCollection
				.distinct("weiboAccount");
		ArrayList<String> weibopassword = (ArrayList<String>) AccountInfoCollection
				.distinct("weiboPassword");
		ArrayList<String> weibotoken = (ArrayList<String>) AccountInfoCollection
				.distinct("weiboToken");
		ArrayList<String> weibosecrettoken = (ArrayList<String>) AccountInfoCollection
				.distinct("weiboTokenSecret");

		if (weiboaccount.size() == 0) {
			return AccountInfo;
		}
		for (int i = 0; i < weiboNumberMax; i++) {
			AccountInfo.add(weiboaccount.get(i));
		}
		for (int i = 0; i < weiboNumberMax; i++) {
			AccountInfo.add(weibopassword.get(i));
		}
		for (int i = 0; i < weiboNumberMax; i++) {
			AccountInfo.add(weibotoken.get(i));
		}
		for (int i = 0; i < weiboNumberMax; i++) {
			AccountInfo.add(weibosecrettoken.get(i));
		}

		return AccountInfo;
	}

}
