package test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;

public class TransferOldData {

	public static DataInputStream usefulResultsread;// Save useful Results just
													// now
	public static DataInputStream nextIDread;
	public static DataOutputStream UniqueUserIDs;
	public static DataOutputStream NextIDs;
	public static DataInputStream resultsread;
	public static DataOutputStream FollowersIDs;

	/**
	 * @param args
	 * @throws IOException
	 */
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		/*
		 * Read the old data of Next ID and the length of the Unique User IDs
		 */
		nextIDread = new DataInputStream(new FileInputStream("nextID.txt"));
		int nextID = nextIDread.readInt() + 1;
		int idlength = nextIDread.readInt();
		nextIDread.close();

		/*
		 * Output the next ID and the length of the Unique User IDs into
		 * NextIDs.txt
		 */
		NextIDs = new DataOutputStream(new FileOutputStream("NextIDs.txt"));
		NextIDs.writeBytes(String.valueOf(nextID));
		NextIDs.writeBytes("\r\n");
		NextIDs.writeBytes(String.valueOf(idlength));
		NextIDs.close();

		/*
		 * Test the Output of the NextIDs.txt
		 */
		DataInputStream testoutput = new DataInputStream(new FileInputStream(
				"NextIDs.txt"));
		int nextID1 = Integer.parseInt(testoutput.readLine());
		int idlength1 = Integer.parseInt(testoutput.readLine());
		if (nextID1 != nextID || idlength != idlength1) {
			System.out.println("Output of the NextIDs.txt is wrong");
		}
		System.out.println(nextID1);
		System.out.println(idlength1);

		/*
		 * Read in old data of Unique User IDs
		 */
		usefulResultsread = new DataInputStream(new FileInputStream(
				"useful results.txt"));
		ArrayList<Integer> UniqueUsersIDList = new ArrayList<Integer>();

		UniqueUsersIDList.add(1774839495);// Important, in the previous results
											// of the Unique User IDs, we
		// forget to record the root node id, here we add the root node to the
		// results.

		for (int i = 0; i < idlength; i++) {
			int id = usefulResultsread.readInt();
			UniqueUsersIDList.add(id);
		}
		usefulResultsread.close();

		/*
		 * Test the validity of the old data
		 */
		System.out.println(UniqueUsersIDList.size());

		/*
		 * Output the Unique User IDs into UniqueUserIDs.txt
		 */
		UniqueUserIDs = new DataOutputStream(new FileOutputStream(
				"UniqueUserIDs.txt"));
		int times = 0;
		for (Integer id : UniqueUsersIDList) {
			UniqueUserIDs.writeInt(id);
			times++;
		}
		System.out.println(times);
		UniqueUserIDs.close();

		/*
		 * Test the output of UniqueUserIDs.txt
		 */
		DataInputStream testoutput1 = new DataInputStream(new FileInputStream(
				"UniqueUserIDs.txt"));
		for (int i = 0; i < idlength; i++) {
			int id = testoutput1.readInt();
			if (id != UniqueUsersIDList.get(i)) {
				System.out.println("Output of UniqueUserIDs.txt is wrong");
			}
		}
		testoutput1.close();
		File file_UniqueUsersIDs = new File("UniqueUserIDs.txt");
		System.out.println("Size of UniqueUsersIDs: "
				+ file_UniqueUsersIDs.length());

		/*
		 * Read the Users' Followers ID
		 */
		ArrayList<Integer> followersID = new ArrayList<Integer>();
		resultsread = new DataInputStream(new FileInputStream("results.txt"));
		loop: for (int i = 0; i < nextID - 1; i++) {
			boolean flag = false;// mark if the followers of ID i is finished
									// reading
			while (!flag) {
				int readin = 0;
				try {
					readin = resultsread.readInt();
				} catch (IOException e) {
					break loop;
				}
				if (readin == -1) {
					flag = true;
				} else {
				}
				followersID.add(readin);
			}
		}
		resultsread.close();

		resultsread = new DataInputStream(new FileInputStream("results1.txt"));
		loop: for (int i = 0; i < nextID - 1; i++) {
			boolean flag = false;// mark if the followers of ID i is finished
									// reading
			while (!flag) {
				int readin = 0;
				try {
					readin = resultsread.readInt();
				} catch (IOException e) {
					break loop;
				}
				if (readin == -1) {
					flag = true;
				} else {
				}
				followersID.add(readin);
			}
		}

		/*
		 * Test the input of Users'Follwers ID
		 */
		ArrayList<Integer> UsersIDList = new ArrayList<Integer>();
		UsersIDList.add(followersID.get(0));
		for (int i = 0; i < followersID.size(); i++) {
			int id = followersID.get(i);
			if (id == -1 && (i + 1) < followersID.size()) {
				UsersIDList.add(followersID.get(i + 1));
			}
		}
		System.out.println(UsersIDList.size());
		for (int i = 0; i < UsersIDList.size(); i++) {
			if (UsersIDList.get(i).intValue() != UniqueUsersIDList.get(i)
					.intValue()) {
				// System.out.println("Reading the Users'followers is wrong");
				System.out
						.println("UniqueUserID: "
								+ UniqueUsersIDList.get(i)
								+ "UserID: "
								+ UsersIDList.get(i)
								+ "Difference: "
								+ (UniqueUsersIDList.get(i) - UniqueUsersIDList
										.get(i)));
			}
		}

		/*
		 * Output the Users'followers ID
		 */
		int FileNumber_FollowersIDs = 0;
		FollowersIDs = new DataOutputStream(new FileOutputStream(
				"FollowersID-0.txt"));
		File file_FollowersID = new File("FollowersID-0.txt");
		for (int i = 0; i < followersID.size(); i++) {
			FollowersIDs.writeInt(followersID.get(i));
			FollowersIDs.flush();
			if (file_FollowersID.length() > 10485760) {
				FollowersIDs.close();
				System.out.println("Size of " + file_FollowersID.toString()
						+ " " + file_FollowersID.length());
				FileNumber_FollowersIDs++;
				FollowersIDs = new DataOutputStream(new FileOutputStream(
						"FollowersID-"
								+ String.valueOf(FileNumber_FollowersIDs)
								+ ".txt"));
				file_FollowersID = new File("FollowersID-"
						+ String.valueOf(FileNumber_FollowersIDs) + ".txt");
			}
		}

		NextIDs = new DataOutputStream(
				new FileOutputStream("NextIDs.txt", true));
		NextIDs.writeBytes("\r\n");
		NextIDs.writeBytes(String.valueOf(FileNumber_FollowersIDs));
		NextIDs.close();
	}

}
