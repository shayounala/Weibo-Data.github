package datamining;

import java.util.ArrayList;
import weibo4j.IDs;
import weibo4j.User;
import weibo4j.Weibo;
import weibo4j.WeiboException;

public class SocialNetwork{
	
	private int Cursormax = 5000;
	private int IDmax = 9999;

	/**
	 * @param weibo The weibo user who takes the application to mine the data
	 * @param user 
	 * @return the followers'ID of user
	 * @throws WeiboException
	 */
	public IDs getfollowersID(Weibo weibo, User user)throws WeiboException{
			return weibo.getFollowersIDSByScreenName(user.getScreenName(), -1);
	}
	
	/**
	 * @param weibo
	 * @param UserID
	 * @return the ids of the followers of user screenName, the maximum number of the followers is 9999
	 * @throws WeiboException
	 */
	public ArrayList<Long> getfollowersIDByUserID(Weibo weibo,String UserID)throws WeiboException{
		int idnumber = 0;
		int iretator = 0;

		ArrayList<Long> followers = new ArrayList<Long>();
		
		long [] id = weibo.getFollowersIDSByUserId(UserID, -1, Cursormax).getIDs();
		idnumber = id.length;
		for(int i=0;i<id.length;i++){
			followers.add(id[i]);
		}
		iretator++;
		
		while(id.length==Cursormax && idnumber<IDmax){
			id = null;
			id = weibo.getFollowersIDSByUserId(UserID, idnumber-1, Cursormax).getIDs();
			if(followers.get(idnumber-1)!=id[0]){
				System.out.println("Mining FollowersIDS fault");
			}
			idnumber = idnumber+id.length-1;
			for(int i=1;i<id.length;i++){
				followers.add(id[i]);
			}
			iretator++;
			System.out.println("idnumber: "+idnumber+"id: "+id.length+"iretator: "+iretator);
		}
		
		
		System.out.println("iretator: "+iretator+" followersidnumber: "+idnumber);
		return followers;
	}
	
	
	/**
	 * @param weibo
	 * @param UserID
	 * @return the ids of the followers of user screenName, the maximum number of the followers is 9999
	 * @throws WeiboException
	 */
	public ArrayList<Long> getfriendsIDByUserID(Weibo weibo,String UserID)throws WeiboException{
		int idnumber = 0;
		int iretator = 0;

		ArrayList<Long> friends = new ArrayList<Long>();
		
		long [] id = weibo.getFriendsIDSByUserId(UserID, -1, Cursormax).getIDs();
		idnumber = id.length;
		for(int i=0;i<id.length;i++){
			friends.add(id[i]);
		}
		iretator++;
		
		while(id.length==Cursormax && idnumber<IDmax){
			id = null;
			id = weibo.getFriendsIDSByUserId(UserID, idnumber-1, Cursormax).getIDs();
			if(friends.get(idnumber-1)!=id[0]){
				System.out.println("Mining FollowersIDS fault");
			}
			idnumber = idnumber+id.length-1;
			for(int i=1;i<id.length;i++){
				friends.add(id[i]);
			}
			iretator++;
			System.out.println("idnumber: "+idnumber+"id: "+id.length+"iretator: "+iretator);
		}
		
		
		System.out.println("iretator: "+iretator+" friendsidnumber: "+idnumber);
		return friends;
	}
	
}
