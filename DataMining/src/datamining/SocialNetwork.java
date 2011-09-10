package datamining;

import java.util.ArrayList;
import weibo4j.IDs;
import weibo4j.User;
import weibo4j.Weibo;
import weibo4j.WeiboException;

public class SocialNetwork{

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
	public long [] getfollowersIDByUserID(Weibo weibo,String UserID)throws WeiboException{
		int idnumber = 0;
		int iretator = 0;
		int Cursormax = 5000;
		int IDmax = 9999;
		long [] ids;
		ArrayList<Long> tempid = new ArrayList<Long>();
		
		long [] id = weibo.getFollowersIDSByUserId(UserID, -1, Cursormax).getIDs();
		idnumber = id.length;
		for(int i=0;i<id.length;i++){
			tempid.add(id[i]);
		}
		iretator++;
		
		while(id.length==Cursormax && idnumber<IDmax){
			id = null;
			id = weibo.getFollowersIDSByUserId(UserID, idnumber-1, Cursormax).getIDs();
			if(tempid.get(idnumber-1)!=id[0]){
				System.out.println("Mining FollowersIDS fault");
			}
			idnumber = idnumber+id.length-1;
			for(int i=1;i<id.length;i++){
				tempid.add(id[i]);
			}
			iretator++;
			System.out.println("idnumber: "+idnumber+"id: "+id.length+"iretator: "+iretator);
		}
		
		ids = new long[idnumber];
		for(int i=0;i<tempid.size();i++){
			ids[i] = tempid.get(i);
		}
		
		System.out.println("iretator: "+iretator);
		return ids;
	}
	
	
	/**
	 * @param weibo
	 * @param UserID
	 * @return the ids of the followers of user screenName, the maximum number of the followers is 9999
	 * @throws WeiboException
	 */
	public long [] getfriendsIDByUserID(Weibo weibo,String UserID)throws WeiboException{
		int idnumber = 0;
		int iretator = 0;
		int Cursormax = 5000;
		int IDmax = 9999;
		long [] ids;
		ArrayList<Long> tempid = new ArrayList<Long>();
		
		long [] id = weibo.getFriendsIDSByUserId(UserID, -1, Cursormax).getIDs();
		idnumber = id.length;
		for(int i=0;i<id.length;i++){
			tempid.add(id[i]);
		}
		iretator++;
		
		while(id.length==Cursormax && idnumber<IDmax){
			id = null;
			id = weibo.getFriendsIDSByUserId(UserID, idnumber-1, Cursormax).getIDs();
			if(tempid.get(idnumber-1)!=id[0]){
				System.out.println("Mining FollowersIDS fault");
			}
			idnumber = idnumber+id.length-1;
			for(int i=1;i<id.length;i++){
				tempid.add(id[i]);
			}
			iretator++;
			System.out.println("idnumber: "+idnumber+"id: "+id.length+"iretator: "+iretator);
		}
		
		ids = new long[idnumber];
		for(int i=0;i<tempid.size();i++){
			ids[i] = tempid.get(i);
		}
		
		System.out.println("iretator: "+iretator);
		return ids;
	}
	
}
