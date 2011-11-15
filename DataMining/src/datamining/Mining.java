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

	public static final int WeiboNumberMax = 4;
	public static int NextFollowerID;
	public static int NextFriendID;
	public static int NextTweetID;
	public static ArrayList<Long> UniqueUserIDList;
	public static final int RateLimitMax = 1000;
	public static final int SingleMaxforUniqueIDs = 10000;
	public static Processing processing;
	public static ImportDataFromMongo importdata;
	public static ExportDataToMongo exportdata;
	public static SocialNetwork socialnetwork;
	public static InformationNetwork informationnetwork;
	public static InitiationforWeibo initiation;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		try {
			processing = new Processing("mydb","AccountInformation1", "NextIDs", "UniqueUserIDs",
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
		initiation.initiations(processing);
		
		
		//test();
		//System.exit(0);
		
		
		socialnetwork_Initiation(initiation);
		//informationnetwork_Initiation(initiation);

	}

	private static void informationnetwork_Initiation(
			InitiationforWeibo initiation2) {
		// TODO Auto-generated method stub
		informationnetwork = new InformationNetwork();
		NextTweetID = importdata.importNextUniqueIDTweet(processing);
		UniqueUserIDList = importdata.importUniqueUserIDs(processing);
		informationnetwork.datamining_Tweets();
		
	}

	private static void socialnetwork_Initiation(InitiationforWeibo initiation) {
		// TODO Auto-generated method stub
		socialnetwork = new SocialNetwork();
		NextFollowerID = importdata.importNextUniqueIDFollowers(processing);
		UniqueUserIDList = importdata.importUniqueUserIDs(processing);
		socialnetwork.datamining_FollowersID();
	}
	

	public static void test() {
		Weibo weibo = initiation.getWeibo(0);

		/*
		 * test the social network mining
		 */
		SocialNetwork socialnetwork = new SocialNetwork();
		try {
			ArrayList<Long> id = socialnetwork.getfollowersIDByUserID(weibo,
					"1774839495");
			for (int i = 0; i < id.size(); i++) {
				System.out.println((i + 1) + ": " + id.get(i).longValue());
			}
			
			
		} catch (WeiboException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		/*
		 * test the tweet mining
		 */
		try {
			InformationNetwork tweet = new InformationNetwork();
			List<Status> statuses = tweet.getUserTimeline(weibo, "1750903687");
			System.out.println(statuses.size());
			int statusesid = 1;
			for (Status status : statuses) {
				System.out.println(statusesid+"   "+status.getUser().getId() + ":"
						+ status.getId() + ":"+status.getCreatedAt().getTime()+" : " 
						+ status.getInReplyToUserId());
				statusesid++;
			}
			
			
			//statuses = tweet.getRepostTimeline(weibo, Long.toString(statuses.get(0).getId()));
			//System.out.println(statuses.get(0));
		} catch (WeiboException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
