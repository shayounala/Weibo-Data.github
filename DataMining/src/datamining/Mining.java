package datamining;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import com.mongodb.MongoException;

import dataprocessing.ExportDataToMongo;
import dataprocessing.ImportDataFromMongo;
import dataprocessing.Processing;
import weibo4j.Status;
import weibo4j.Weibo;
import weibo4j.WeiboException;

public class Mining {

	public static final int WeiboNumberMax = 14;
	public static int NextFollowerID;
	public static int NextFriendID;
	public static int NextTweetID;
	public static ArrayList<Long> UniqueUserIDList;
	public static final int RateLimitMax = 1000;
	public static Processing processing;
	public static ImportDataFromMongo importdata;
	public static ExportDataToMongo exportdata;
	public static SocialNetwork socialnetwork;
	public static InitiationforWeibo initiation;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//test();
		
		try {
			processing = new Processing("mydb","AccountInformaiton", "NextIDs", "UniqueUserIDs",
					"UserInformation", "UniqueTweetIDs", "TweetInformation");
			processing.initiations();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MongoException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		importdata = new ImportDataFromMongo();
		exportdata = new ExportDataToMongo();
		
		initiation = new InitiationforWeibo(WeiboNumberMax);
		initiation.afterinitiations(processing);
		System.exit(0);
		
		socialnetwork_Initiation(initiation);

	}

	private static void socialnetwork_Initiation(InitiationforWeibo initiation) {
		// TODO Auto-generated method stub
		socialnetwork = new SocialNetwork();
		NextFollowerID = importdata.importNextUniqueIDFollowers(processing);
		UniqueUserIDList = importdata.importUniqueUserIDs(processing);
		socialnetwork.datamining_FollowersID();
	}
	

	public static void test() {
		Weibo weibo = InitiationforWeibo.intiation();

		/*
		 * test the social network mining
		 */
		SocialNetwork socialnetwork = new SocialNetwork();
		try {
			ArrayList<Long> id = socialnetwork.getfollowersIDByUserID(weibo,
					"1774839495");
			for (int i = 0; i < id.size(); i++) {
				System.out.println((i + 1) + ": " + id.get(i));
			}
		} catch (WeiboException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		/*
		 * test the tweet mining
		 */
		try {
			Tweet tweet = new Tweet();
			List<Status> statuses = tweet.getUserTimeline(weibo, "1774839495");
			System.out.println(statuses.size());
			for (Status status : statuses) {
				System.out.println(status.getUser().getId() + ":"
						+ status.getId() + ":" + status.getText()
						+ status.getRetweeted_status());
			}
		} catch (WeiboException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
