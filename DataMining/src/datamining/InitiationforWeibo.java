package datamining;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

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
		WeiboList = new ArrayList<Weibo>();
	}

	public  int WeiboNumberMax;
	private  String [] weiboAccount;//Tokens of weibo account
	private  String [] weiboPassword;//Tokensecrets of weibo account
	private  ArrayList<Weibo> WeiboList;
	
	
	public Weibo getWeiboList(int WeiboNumber) {
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
	
	public  void initiations(){
		
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
				weiboAccount[i] = accessToken.getToken();
				weiboPassword[i] = accessToken.getTokenSecret();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}catch (WeiboException we){
				we.printStackTrace();
			}
		}
		
		
	}
	
}
