package datamining;

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
	public List<Status> getUserTimeline(Weibo weibo, String screenName)throws WeiboException{
		int Countmax = 200;
		
		Paging paging = new Paging();
		paging.setCount(Countmax);
		
		List<Status> statuses=weibo.getUserTimeline(screenName,0,1,paging);
		paging.setPage(2);
		statuses.addAll(weibo.getUserTimeline(screenName,0,1, paging));
		
		return statuses;
	}
	
	/**
	 * @param weibo
	 * @param id
	 * @return the latest retweet for tweet id with maximum number of Countmax
	 * @throws WeiboException
	 */
	public List<Status> getRepostTimeline(Weibo weibo, String id)throws WeiboException{
		int Countmax = 200;
		
		Paging paging = new Paging();
		paging.setCount(Countmax);
		List<Status> statuses = weibo.getreposttimeline(id, paging);
		
		return statuses;
	}
}
