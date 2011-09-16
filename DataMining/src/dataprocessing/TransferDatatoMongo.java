package dataprocessing;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;


import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.QueryOperators;

public class TransferDatatoMongo {
	
	public static DataInputStream usefulResultsread;//Save useful Results just now
	public static DataInputStream nextIDread;
	public static DataInputStream resultsread;

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		
		try {
			Mongo mongo = new Mongo("localhost",27017);
			mongo.dropDatabase("testdb");
			//System.exit(0);
			DB db = mongo.getDB("testdb");	
			//System.exit(0);

			
			/*Read the old data of Next ID and the length of the Unique User IDs
			 * 
			 */
			nextIDread = new DataInputStream(new FileInputStream("nextID.txt"));
			int nextID = nextIDread.readInt()+1;
			int idlength = nextIDread.readInt();
			nextIDread.close();
			
			/*
			 * Output the next ID and the length of the Unique User IDs into NextIDs.txt
			 */
			DBCollection NextIDsCollection = db.getCollection("NextIDs");
			//System.exit(0);
			BasicDBObject NextIDsObject = new BasicDBObject();
			NextIDsObject.put("Next ID for Mining followers ID",nextID);
			NextIDsObject.put("Next ID for Mining friends ID",-1);
			NextIDsObject.put("Next ID for Mining Tweets",-1);
			NextIDsObject.put("Next Tweet ID for Mining Reposts",-1);
			NextIDsCollection.insert(NextIDsObject);
			//System.exit(0);
			
			
			/*
			 * Test the Output of the NextIDs.txt
			 */
			DBCursor testnextids = NextIDsCollection.find();
			System.out.println(testnextids.toArray().size());
			System.out.println(testnextids.toArray().get(0));
			//System.exit(0);
			
			/*
			 * Read in old data of Unique User IDs
			 */
			usefulResultsread = new DataInputStream(new FileInputStream("useful results.txt"));
			ArrayList<Integer> UniqueUsersIDList = new ArrayList<Integer>();
			
			
			
			UniqueUsersIDList.add(1774839495);//Important, in the previous results of the Unique User IDs, we 
			//forget to record the root node id, here we add the root node to the results.
			
			
			
			for(int i=0;i<idlength;i++){
				int id = usefulResultsread.readInt();
				//System.out.println(i);
				if(!UniqueUsersIDList.contains(id)){
					UniqueUsersIDList.add(id);
				}
			}
			usefulResultsread.close();
			
			/*
			 * Test the validity of the old data
			 */
			System.out.println(UniqueUsersIDList.size());
			for(int i=0;i<UniqueUsersIDList.size();i++){
				for(int j=i+1;j<UniqueUsersIDList.size();j++){
					if(UniqueUsersIDList.get(i)==UniqueUsersIDList.get(j)){
						System.out.println("False ID: Same ID in the getFollowersIDs");
						System.exit(0);
					}
				}
			}
			System.out.println(UniqueUsersIDList.size());
			/*
			 * Read the Users' Followers ID
			 */
			ArrayList<Integer> followersID = new ArrayList<Integer>();
			resultsread = new DataInputStream(new FileInputStream("results.txt"));
			loop:for(int i=0;i<nextID-1;i++){
				boolean flag = false;//mark if the followers of ID i is finished reading
				while(!flag){
					int readin = 0;
					try{
						readin = resultsread.readInt();
					}catch (IOException e){
						break loop;
					}
					if(readin == -1){
						flag = true;
					}else{
					}
					followersID.add(readin);
				}
			}
			resultsread.close();
			
			resultsread = new DataInputStream(new FileInputStream("results1.txt"));
			loop:for(int i=0;i<nextID-1;i++){
				boolean flag = false;//mark if the followers of ID i is finished reading
				while(!flag){
					int readin = 0;
					try{
						readin = resultsread.readInt();
					}catch (IOException e){
						break loop;
					}
					if(readin == -1){
						flag = true;
					}else{
					}
					followersID.add(readin);
				}
			}
			
			/*
			 * Test the input of Users'Follwers ID
			 */
			/*ArrayList<Integer> UsersIDList = new ArrayList<Integer>();
			UsersIDList.add(followersID.get(0));
			for(int i=0;i<followersID.size();i++){
				int id = followersID.get(i);
				if(id == -1 && (i+1)<followersID.size()){
					if(!UsersIDList.contains(followersID.get(i+1))){
						UsersIDList.add(followersID.get(i+1));
					}
				}
			}
			System.out.println(UsersIDList.size());
			for(int i=0;i<UsersIDList.size();i++){
				if(UsersIDList.get(i).intValue() != UniqueUsersIDList.get(i).intValue()){
					//System.out.println("Reading the Users'followers is wrong");
					System.out.println("UniqueUserID: "+UniqueUsersIDList.get(i)+"UserID: "+UsersIDList.get(i)+"Difference: "+(UniqueUsersIDList.get(i)-UniqueUsersIDList.get(i)));
				}
			}*/
			
			
			
			
			/*
			 * Output the Unique User IDs into UniqueUserIDs.txt
			 */
			
			/*BasicDBObject query = new BasicDBObject();
			query.put("Next ID for Mining followers ID", new BasicDBObject(QueryOperators.EXISTS, true));
			System.out.println(NextIDsCollection.find(query).toArray().get(0).get("Next ID for Mining followers ID"));
			int nextid = UniqueUsersIDList.get(NextIDsCollection.find(query).toArray().get(0).get("Next ID for Mining followers ID").hashCode());
			NextIDsCollection.update(query, new BasicDBObject("Next ID for Mining followers ID",nextid));
			System.out.println(NextIDsCollection.find(query).toArray().get(0).get("Next ID for Mining followers ID"));
			System.exit(0)*/;
			
			
			
			DBCollection UniqueUserIDsCollection = db.getCollection("UniqueUserIDs");

			for(int i=0;i<UniqueUsersIDList.size();i++){
				UniqueUserIDsCollection.insert(new BasicDBObject("Number",i).append("ID", UniqueUsersIDList.get(i).intValue()));
				//UniqueUserIDsCollection.insert(new BasicDBObject().append("Unique User ID", UniqueUsersIDList.get(i).intValue()));
			}
			//System.exit(0);
			
			/*
			 * Test the output of UniqueUserIDs.txt
			 */
			System.out.println(UniqueUserIDsCollection.find().count());
			//System.exit(0);
			
			
			/*
			 * Output the Users'followers ID
			 */

			DBCollection UserInformationCollection = db.getCollection("UserInformation");
			ArrayList<Integer> tempfollowersID = new ArrayList<Integer>();
			ArrayList<Integer> UserID = new ArrayList<Integer>();
			int number =0;
			int totalnumber = 0;
			for(int i=0;i<followersID.size();i++){
				int id = followersID.get(i);
				if(id!=-1){
					tempfollowersID.add(id);
				}
				if(id == -1 && !UserID.contains(tempfollowersID.get(0))){
					UserID.add(tempfollowersID.get(0));
					//BasicDBObject query = new BasicDBObject("Number",number);
					BasicDBObject userfollowerobject = new BasicDBObject();//(BasicDBObject)UniqueUserIDsCollection.findOne(query);
					userfollowerobject.put("ID", tempfollowersID.get(0));
					tempfollowersID.remove(0);
					userfollowerobject.put("Followers ID", tempfollowersID);
					UserInformationCollection.insert(userfollowerobject);
					number++;
					totalnumber = totalnumber+tempfollowersID.size();
					System.out.println(number);
					tempfollowersID.clear();
				}else if(id == -1){
					tempfollowersID.clear();
				}
			}
			
			/*
			 * Test the output of Users'Follwers ID
			 */
			System.out.println("totalnumber"+totalnumber);
			System.out.println(UserInformationCollection.find(new BasicDBObject("ID",new BasicDBObject(QueryOperators.EXISTS,true))).count());

			
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MongoException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			e.getCode();
		}
		

	}

}
