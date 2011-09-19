package dataprocessing;

public class TestNetwork {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		double N1 = 100;
		double N2 = 100;
		double ave1 = 5;
		double ave2 = 5;
		double ratio1 = 1;
		double ratio2 = 1;
		
		double ave3 = 0;
		for(int i=0;i<10000;i++){
			double sum3 = (double)N1*N2*(ave1*ratio1+ave2*ratio2+1)+N1*(N1-1)*ave1/2+N2*(N2-1)*ave2/2;
			
			double NN3 = (N1+N2)*(N1+N2-1)/2;
			ave3 = sum3/NN3;
			N1 = N1+N2;
			ave1 = ave3;
			ratio1 = (ratio1*ave1*(N1-1)+1+(ratio2*ave2+1)*(N2-1))/((N1+N2-1)*ave3);
			System.out.println("ave3: "+ave3+" ratio1: "+ratio1+" sum3: "+sum3+" NN3: " +NN3+ " N1: "+N1+" N2: "+N2+" ave1: "+ave1+" ave2: "+ave2);
		}
		
		
		

	}

}
