package dataprocessing;

import java.net.UnknownHostException;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.QueryOperators;

public class Processing {

	public Processing(String dbname, String AccountInformation, String NextIDs,
			String UniqueUserIDs, String UserInformation,
			String UniqueTweetIDs, String TweetInformation)
			throws UnknownHostException, MongoException {
		super();
		this.dbname = dbname;
		this.AccountInformation = AccountInformation;
		this.NextIDs = NextIDs;
		this.UniqueUserIDs = UniqueUserIDs;
		this.UserInformation = UserInformation;
		this.UniqueTweetIDs = UniqueTweetIDs;
		this.TweetInformation = TweetInformation;
		this.mongo = new Mongo("10.3.4.84", 27017);
        mongo.getDB(dbname).authenticate("cssc", new char[]{'1'});

	}

	private String dbname;
	private String AccountInformation;
	private String NextIDs;
	private String UniqueUserIDs;
	private String UserInformation;
	private String UniqueTweetIDs;
	private String TweetInformation;

	private Mongo mongo;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	public DB getDB() {
		// TODO Auto-generated method stub
		return mongo.getDB(dbname);
	}

	public String getUniqueUserIDs() {
		// TODO Auto-generated method stub
		return UniqueUserIDs;
	}

	public String getUserInformation() {
		return UserInformation;
	}

	public String getNextIDs() {
		// TODO Auto-generated method stub
		return NextIDs;
	}

	public String getAccountInformation() {
		return AccountInformation;
	}

	public String getUniqueTweetIDs() {
		// TODO Auto-generated method stub
		return UniqueTweetIDs;
	}

	public String getTweetInformation() {
		return TweetInformation;
	}

	public void initiations() {
		// TODO Auto-generated method stub

		//dropCollections();
		DBCollection NextIDsCollection = getDB().getCollection(
				this.getNextIDs());
		
		/*
		 * If the initiation is ever executed,then return 
		 */
		if (null != NextIDsCollection.findOne(new BasicDBObject(
				"Next ID for Mining followers ID", new BasicDBObject(
						QueryOperators.EXISTS, true)))) {
			return;
		}
		
		
		
		BasicDBObject NextIDsObject = new BasicDBObject();
		NextIDsObject.put("Next ID for Mining followers ID", 0);
		NextIDsObject.put("Next ID for Mining friends ID", 0);
		NextIDsObject.put("Next ID for Mining Tweets", 0);
		NextIDsObject.put("Next Tweet ID for Mining Reposts", 0);
		NextIDsCollection.insert(NextIDsObject);

		DBCollection UniqueUserIDsCollection = getDB().getCollection(
				this.getUniqueUserIDs());
		
		
		BasicDBObject initialUser = new BasicDBObject();
		initialUser.put("User Number", 0);
		initialUser.put("User ID", ((long) 1774839495));
		UniqueUserIDsCollection.insert(initialUser);

	}

	private void dropCollections() {
		// TODO Auto-generated method stub
		DBCollection NextIDsCollection = getDB().getCollection(
				this.getNextIDs());
		NextIDsCollection.drop();
		
		DBCollection UniqueUserIDsCollection = getDB().getCollection(
				this.getUniqueUserIDs());
		UniqueUserIDsCollection.drop();
		
		DBCollection UserInformationCollection = getDB().getCollection(
				this.getUserInformation());
		UserInformationCollection.drop();
	}

}
