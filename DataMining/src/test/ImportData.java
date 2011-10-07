package test;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;

public class ImportData {

	/**
	 * @param FileName of the UniqueUserIDs
	 * @param NextIDs
	 * @return the list of the Unique User IDs 
	 */
	public ArrayList<Integer> importUniqueUserIDs(String FileName,String NextIDs){
		
		ArrayList<Integer> UniqueUserIDs = new ArrayList<Integer>();
		
		/*
		 * Read the length of the UniqueUserIDs
		 */
		DataInputStream NextIDsInput;
		int idLength = 0;
		try {
			NextIDsInput = new DataInputStream(new FileInputStream(NextIDs));
			NextIDsInput.readLine();
			idLength = Integer.parseInt(NextIDsInput.readLine());
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		/*
		 *Test the validity of the length of UniqueUserIDs 
		 */
		if(idLength == 0){
			System.out.println("Reading the length of the UnqiqueUserIDs is wrong");
		}
		
		/*
		 * Read the data of the UniqueUserIDs
		 */
		try {
			DataInputStream UniqueUserIDsInput = new DataInputStream(new FileInputStream(FileName));
			for(int i=0;i<idLength;i++){
				UniqueUserIDs.add(UniqueUserIDsInput.readInt());
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			System.out.println("File Name is wrong in importUniqueUserIDs");
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		return UniqueUserIDs;
	}
	
	/**
	 * @param NextIDs
	 * @return the next ID for data mining
	 */
	public int importNextID(String NextIDs){
		int nextID = 0;
		
		/*
		 * Read the NextID
		 */
		DataInputStream NextIDsInput;
		try {
			NextIDsInput = new DataInputStream(new FileInputStream(NextIDs));
			nextID = Integer.parseInt(NextIDsInput.readLine());
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		/*
		 * Test the NextID
		 */
		if(nextID == 0){
			System.out.println("Reading the NextID is wrong");
		}
		
		return nextID;
	}
	
	/**
	 * @param NextIDs
	 * @return the file number of the followersID-*.txt
	 */
	public int importFileNumber_FollowersID(String NextIDs){
		int FileNumber_FollowersID = 0;
		
		DataInputStream NextIDsInput;
		try {
			NextIDsInput = new DataInputStream(new FileInputStream(NextIDs));
			NextIDsInput.readLine();
			NextIDsInput.readLine();
			FileNumber_FollowersID = Integer.parseInt(NextIDsInput.readLine());
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
		}
		
		return FileNumber_FollowersID;
	}
}
