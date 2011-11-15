package datamining;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import dataprocessing.ExportDataToMongo;
import dataprocessing.ImportDataFromMongo;
import dataprocessing.Processing;

import weibo4j.User;
import weibo4j.Weibo;
import weibo4j.WeiboException;
import weibo4j.http.AccessToken;
import weibo4j.http.RequestToken;
import weibo4j.util.BareBonesBrowserLaunch;

public class InitiationforWeibo {
	
	public InitiationforWeibo(int weiboNumberMax) {
		super();
		WeiboNumberMax = weiboNumberMax;
		weiboAccount = new String [WeiboNumberMax];
		weiboPassword = new String [WeiboNumberMax];
		weiboToken = new String [WeiboNumberMax];
		weiboTokenSecret = new String [WeiboNumberMax];
		WeiboList = new ArrayList<Weibo>();
		newApplication();
		
		System.setProperty("weibo4j.oauth.consumerKey", Weibo.CONSUMER_KEY);
    	System.setProperty("weibo4j.oauth.consumerSecret", Weibo.CONSUMER_SECRET);
	}

	/**
	 * Mining data with another application.
	 * In details, we just change the consumer_key and consumer_secret in Class Weibo.
	 */
	private void newApplication() {
		// TODO Auto-generated method stub
		Weibo.CONSUMER_KEY = "1372154034";
		Weibo.CONSUMER_SECRET = "c79efb329eb95baa1bd2f515820efe2d";
	}

	public  int WeiboNumberMax;//the maximum number of weibo for mining
	private  String [] weiboAccount;//weibo account
	private  String [] weiboPassword;//weibo password
	private  String [] weiboToken;//Tokens of weibo account
	private  String [] weiboTokenSecret;//Tokensecrets of weibo account
	private  ArrayList<Weibo> WeiboList;//objects of weibo using for mining
	
	
	public Weibo getWeibo(int WeiboNumber) {
		return WeiboList.get(WeiboNumber);
	}

	public void setWeiboList(int index, Weibo weibo) {
		WeiboList.set(index, weibo);
	}

	public InitiationforWeibo(){
		
	}

	/**
	 * @return an object weibo 
	 */
	public static Weibo intiation(){
		System.setProperty("weibo4j.oauth.consumerKey", Weibo.CONSUMER_KEY);
    	System.setProperty("weibo4j.oauth.consumerSecret", Weibo.CONSUMER_SECRET);
    	
        Weibo weibo = new Weibo();
        
        weibo.setToken("b84ea958dc97658992bcfb985015337c", "a5d965b3919883c66d03802e5a476715");
        return weibo;
	}
	
	/**
	 * @param processing
	 * initiate the WeiboList with weibos, the number of weibos is WeiboNumberMax
	 */
	public void initiations(Processing processing){
		/*
		 * If the information of Weibo Account is initiated before, return the results of afterinitiations,
		 * otherwise, fetch the information of Weibo Account by httpOauth.
		 */
		if(afterinitiations(processing)){
			return;
		}
			
		for(int i=0;i<WeiboNumberMax;i++){
			try {
				WeiboList.add(new Weibo());
				RequestToken requestToken = WeiboList.get(i).getOAuthRequestToken();

				System.out.println("Got request token.");
				System.out.println("Request token: "+ requestToken.getToken());
				System.out.println("Request token secret: "+ requestToken.getTokenSecret());
				AccessToken accessToken = null;

				BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
				while (null == accessToken) {
					System.out.println("Open the following URL and grant access to your account:");
					System.out.println(requestToken.getAuthorizationURL());
					BareBonesBrowserLaunch.openURL(requestToken.getAuthorizationURL());
					System.out.print("Hit enter when it's done.[Enter]:");

					String pin = br.readLine();
					System.out.println("pin: " + br.toString());
					try{
						accessToken = requestToken.getAccessToken(pin);
					} catch (WeiboException te) {
						if(401 == te.getStatusCode()){
							System.out.println("Unable to get the access token.");
						}else{
							te.printStackTrace();
						}
					}
				}
				weiboAccount[i] = "datamining"+String.valueOf(i);
				weiboToken[i] =	accessToken.getToken();
				weiboPassword[i] = "datamining"+String.valueOf(i);
				weiboTokenSecret[i]	= accessToken.getTokenSecret();
				
				System.out.println(WeiboList.get(i).showUser("David_Scoot").toString());
				ExportDataToMongo export = new ExportDataToMongo();
				export.ExportAccountInformation(processing,weiboAccount[i],weiboPassword[i],weiboToken[i],weiboTokenSecret[i]);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}catch (WeiboException we){
				we.printStackTrace();
			}
		}
		
		
	}

	/**
	 * @param processing
	 * @return whether the account information is fetched by HttpOauth at the first time
	 * if the return is true, initiate weibo accounts from the database
	 */
	public boolean afterinitiations(Processing processing) {
		// TODO Auto-generated method stub
		boolean isInitialed = true;
		
		
		ImportDataFromMongo imports = new ImportDataFromMongo();
		ArrayList<String> AccountInfo = imports.importAccountInfomation(processing, WeiboNumberMax);
		
		
		if(AccountInfo.size() == 0){
			isInitialed = false;
			return isInitialed;
		}
		
		for(int i=0;i<AccountInfo.size();i++){
			
			switch(i/this.WeiboNumberMax){
			case 0:
				weiboAccount[i] = AccountInfo.get(i);
			case 1:
				weiboPassword[i%this.WeiboNumberMax] = AccountInfo.get(i);
			case 2:
				weiboToken[i%this.WeiboNumberMax] = AccountInfo.get(i);
			case 3:
				weiboTokenSecret[i%this.WeiboNumberMax] = AccountInfo.get(i);
				
				System.out.println(AccountInfo.get(i));
			}
		}
		
		
		for(int i=0;i<this.WeiboNumberMax;i++){
			Weibo weibo = new Weibo();
	        
	        weibo.setToken(weiboToken[i], weiboTokenSecret[i]);

	        try {
				User user = weibo.verifyCredentials();
				System.out.println(user.toString());
			} catch (WeiboException e) {
				// TODO Auto-generated catch block
				System.out.println(i);
				e.printStackTrace();
			}
	        
	        WeiboList.add(weibo);
		}
		
		System.out.println(isInitialed);
		return isInitialed;
	}
	
}
