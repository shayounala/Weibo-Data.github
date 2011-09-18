package dataprocessing;

import java.net.UnknownHostException;

import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

public class Processing {

	public Processing(String dbname, String AccountInformation, String NextIDs, String UniqueUserIDs,
			String UserInformation) throws UnknownHostException, MongoException {
		super();
		this.dbname = dbname;
		this.AccountInformation = AccountInformation;
		this.NextIDs = NextIDs;
		this.UniqueUserIDs = UniqueUserIDs;
		this.UserInformation = UserInformation;
		this.mongo = new Mongo();
		;
	}

	private String dbname;
	private String AccountInformation;
	private String NextIDs;
	private String UniqueUserIDs;
	private String UserInformation;
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

	public String getUserinformation() {
		return UserInformation;
	}

	public String getNextIDs() {
		// TODO Auto-generated method stub
		return NextIDs;
	}
	
	public String getAccountInformation() {
		return AccountInformation;
	}

}
