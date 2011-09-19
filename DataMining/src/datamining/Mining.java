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
	private static int NextID;
	private static ArrayList<Long> UniqueUserIDList;
	private static final int RateLimitMax = 1000;
	private static Processing processing;
	private static ImportDataFromMongo importdata = new ImportDataFromMongo();
	private static ExportDataToMongo exportdata = new ExportDataToMongo();

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
		
		InitiationforWeibo initiation = new InitiationforWeibo(WeiboNumberMax);
		initiation.afterinitiations(processing);
		System.exit(0);
		
		socialnetwork_Initiation(initiation);

	}

	private static void socialnetwork_Initiation(InitiationforWeibo initiation) {
		// TODO Auto-generated method stub
		SocialNetwork socialnetwork = new SocialNetwork();
		NextID = importdata.importNextUniqueIDFollowers(processing);
		UniqueUserIDList = importdata.importUniqueUserIDs(processing);
		datamining_FollowersID(socialnetwork, initiation);
	}

	private static void datamining_FollowersID(SocialNetwork socialnetwork,
			InitiationforWeibo initiation) {
		// TODO Auto-generated method stub
		for (int i = 0; i < WeiboNumberMax; i++) {
			Weibo weibo = initiation.getWeiboList(i);
			int RateLimitRemain = RateLimitMax;
			for (int j = NextID; j < UniqueUserIDList.size(); j++) {
				try {
					ArrayList<Long> followers = new ArrayList<Long>();
					RateLimitRemain = weibo.getRateLimitStatus()
							.getRateLimitRemaining();
					if (RateLimitRemain > 0) {
						followers = socialnetwork.getfollowersIDByUserID(weibo,
								UniqueUserIDList.get(j).toString());
					}
					NextID++;
					exportdata.ExportNextUniqueIDFollowers(processing, NextID);
					exportdata.ExportUniqueUserIDs(processing, followers);
					exportdata.ExportUserFollowersID(processing, UniqueUserIDList.get(j), followers);
				} catch (WeiboException e) {
					// TODO Auto-generated catch block
					System.out.println("RateLimitRemain: " + RateLimitRemain);
					e.printStackTrace();
				}

			}
		}
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
