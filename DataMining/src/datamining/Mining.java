package datamining;

import java.util.ArrayList;
import java.util.List;

import dataprocessing.ImportData;
import weibo4j.Status;
import weibo4j.Weibo;
import weibo4j.WeiboException;

public class Mining {

	public static final int WeiboNumberMax = 15;
	private static int FileNumber_FollowersID;
	private static int NextID;
	private static ArrayList<Integer> UniqueUserIDList;
	private static final int RateLimitMax = 1000;
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		test();
		InitiationforWeibo initiation = new InitiationforWeibo(WeiboNumberMax);
		initiation.initiations();
		socialnetwork_Initiation(initiation);
		

	}
	
	
	private static void socialnetwork_Initiation(InitiationforWeibo initiation) {
		// TODO Auto-generated method stub
		SocialNetwork socialnetwork = new SocialNetwork();
		ImportData importdata = new ImportData();
		FileNumber_FollowersID = importdata.importFileNumber_FollowersID("NextIDs");
		NextID = importdata.importNextID("NextIDs");
		UniqueUserIDList = importdata.importUniqueUserIDs("UniqueUserIDs", "NextIDs");
		datamining_FollowersID(socialnetwork,initiation);
	}


	private static void datamining_FollowersID(SocialNetwork socialnetwork, InitiationforWeibo initiation) {
		// TODO Auto-generated method stub
		for(int i=0;i<WeiboNumberMax;i++){
			Weibo weibo = initiation.getWeiboList(i);
			int RateLimitRemain = RateLimitMax ;
			for(int j=NextID;j<UniqueUserIDList.size();j++){
				try {
					RateLimitRemain = weibo.getRateLimitStatus().getRateLimitRemaining();
					if(RateLimitRemain>0){
						socialnetwork.getfollowersIDByUserID(weibo, UniqueUserIDList.get(j).toString());
					}
				} catch (WeiboException e) {
					// TODO Auto-generated catch block
					System.out.println("RateLimitRemain: "+RateLimitRemain);
					e.printStackTrace();
				}
				
			}
		}
	}


	public static void test(){
		Weibo weibo = InitiationforWeibo.intiation();
		
		/*
		 * test the social network mining
		 */
		SocialNetwork socialnetwork = new SocialNetwork();	
		try {
			long [] id = socialnetwork.getfollowersIDByUserID(weibo, "1774839495");
			for(int i=0;i<id.length;i++){
				System.out.println((i+1)+": "+id[i]);
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
			List<Status> statuses=tweet.getUserTimeline(weibo,"1774839495");
			System.out.println(statuses.size());
			for (Status status : statuses) {
	            System.out.println(status.getUser().getId() + ":" +status.getId()+":"+
	                               status.getText()+ status.getRetweeted_status());
	        }
		} catch (WeiboException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}