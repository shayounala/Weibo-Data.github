package test;

public class TestNetwork {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		int number = 10000;
		double [] N = new double [number];
		N[0] = 100;
		N[1] = 100;
		double [] ave = new double [number];
		ave[0] = 5;
		ave[1] = 3;
		double [] ratio = new double [number];
		ratio[0] = 1;
		ratio[1] = 1;
		
		for(int i=0;i<9998;i++){
			N[i+2] = N[0]+N[i+1];
			double sum3 = (double)N[0]*N[i+1]*(ave[0]*ratio[0]+ave[i+1]*ratio[i+1]+1)+N[0]*(N[0]-1)*ave[0]/2+N[i+1]*(N[i+1]-1)*ave[i+1]/2;
			
			double NN3 = N[i+2]*(N[i+2]-1)/2;
			ave[i+2] = sum3/NN3;
			System.out.println(((ratio[0]*ave[0]+1)*(N[0]-1)+1+(ratio[i+1]*ave[i+1])*(N[i+1]-1)));
			System.out.println(((N[i+2]-1)*ave[i+2]));
			ratio[i+2] = ((ratio[0]*ave[0]+1)*(N[0]-1)+1+(ratio[i+1]*ave[i+1])*(N[i+1]-1))/((N[i+2]-1)*ave[i+2]);
			System.out.println("ave3: "+ave[i+2]+" ratio1: "+ratio[i+2]+" sum3: "+sum3+" NN3: " +NN3+ " N1: "+N[0]+" N2: "+N[i+1]+" ave1: "+ave[0]+" ave2: "+ave[i+1]);
		}
		
		
		

	}

}
