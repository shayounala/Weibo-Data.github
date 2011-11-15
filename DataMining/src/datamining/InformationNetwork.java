package datamining;

import java.util.ArrayList;
import java.util.List;

import weibo4j.Paging;
import weibo4j.Status;
import weibo4j.Weibo;
import weibo4j.WeiboException;

public class InformationNetwork {

	/**
	 * @param weibo
	 * @param screenName
	 * @return the latest tweet for user screenName with maximum number of Countmax
	 * @throws WeiboException
	 */
	public ArrayList<Status> getUserTimeline(Weibo weibo, String screenName)throws WeiboException{
		int Countmax = 200;
		
		Paging paging = new Paging();
		paging.setCount(Countmax);
		paging.setSinceId(3363472209835882L);
		
		ArrayList<Status> statuses=(ArrayList<Status>) weibo.getUserTimeline(screenName,0,1,paging);
		for(int i = 2; i<4; i++){
			if(statuses.size()==200*(i-1)){
				paging.setPage(i);
				statuses.addAll(weibo.getUserTimeline(screenName,0,1, paging));
			}else{
				break;
			}
		}
		
		
		return statuses;
	}
	
	/**
	 * @param weibo
	 * @param id
	 * @return the latest retweet for tweet id with maximum number of Countmax
	 * @throws WeiboException
	 */
	public ArrayList<Status> getRepostTimeline(Weibo weibo, String id)throws WeiboException{
		int Countmax = 200;
		
		Paging paging = new Paging();
		paging.setCount(Countmax);
		ArrayList<Status> statuses = (ArrayList<Status>) weibo.getreposttimeline(id, paging);
		for(int i = 2; i<4; i++){
			if(statuses.size()==200*(i-1)){
				paging.setPage(i);
				statuses.addAll(weibo.getreposttimeline(id, paging));
			}else{
				break;
			}
		}
		
		return statuses;
	}

	public void datamining_Tweets() {
		// TODO Auto-generated method stub
		for(int i = 0;i<Mining.WeiboNumberMax;i++){
			Weibo weibo = Mining.initiation.getWeibo(i);
			int RateLimitRemain = Mining.RateLimitMax;
			loop: for(int j=Mining.NextTweetID; j<Mining.UniqueUserIDList.size();j++){
				try{
					ArrayList<Status> tweets = new ArrayList<Status>();
					ArrayList<Long> TweetsID = new ArrayList<Long>();
					ArrayList<Long> TweetsTime = new ArrayList<Long>();
					ArrayList<Status> reposts = new ArrayList<Status>();
					ArrayList<Long> RepostsID = new ArrayList<Long>();
					ArrayList<Long> RepostsUserID = new ArrayList<Long>();
					ArrayList<Long> RepostsTime = new ArrayList<Long>();
					System.out.println("WeiboAccount: "+i);
					if(RateLimitRemain > 0){
						tweets = getUserTimeline(weibo, String.valueOf(Mining.UniqueUserIDList.get(j).longValue()));
						for(Status tweet: tweets){
							TweetsID.add(tweet.getId());
							TweetsTime.add(tweet.getCreatedAt().getTime());
							reposts = getRepostTimeline(weibo, String.valueOf(tweet.getId()));
							for(Status repost: reposts){
								RepostsID.add(repost.getId());
								RepostsUserID.add(repost.getUser().getId());
								RepostsTime.add(repost.getCreatedAt().getTime());
							}
							reposts.clear();
							RepostsID.add(-1L);
							RepostsUserID.add(-1L);
							RepostsTime.add(-1L);
						}
					}
					
					Mining.NextTweetID++;
					Mining.exportdata.ExportNextUniqueIDTweet(Mining.processing, Mining.NextTweetID);
					Mining.exportdata.ExportUserTweetsID(Mining.processing, Mining.UniqueUserIDList.get(j), TweetsID, TweetsTime, RepostsID, RepostsUserID, RepostsTime);
					
				}catch (WeiboException e){
					System.out.println("RateLimitRemain: "+RateLimitRemain);
					e.printStackTrace();
					break loop;
				}
			}
		}
	}
}
