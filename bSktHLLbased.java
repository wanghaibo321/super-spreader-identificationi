package bsktHLLbased;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;



/** A general framework for count min. The elementary data structures to be shared here can be counter, bitmap, FM sketch, HLL sketch. Specifically, we can
 * use counter to estimate flow sizes, and use bitmap, FM sketch and HLL sketch to estimate flow cardinalities
 * @author Jay, Youlin, 2018. 
 */

public class bSktHLLbased {
	public static Random rand = new Random();
	public static int t;

	public static int n = 0; 						// total number of packets
	public static int flows = 0; 					// total number of flows
	public static int avgAccess = 0; 				// average memory access for each packet
	public static final int M = 1024 * 10000; 	// total memory space Mbits	
	public static GenrealHLL[][] C;
	public static long [][] Key;
	public static int[][] Value;
	public static double base = 0.5, b = 1.08;
	public static Set<Integer> expConfig = new HashSet<>(Arrays.asList()); //0-ECountMin dist exp
	public static boolean isGetThroughput = false;
	public static long[] flowida = new long[60000000];
	public static long[] elementida = new long[60000000];
	public static int pkt = 0,  kkk = 0;
	public static int superspreader = 0, falsepositive =0, falsenegative =0, SSthreshold = 400, SSReported =0, ep=10;
	public static HashMap<Long, Double> realSpread = new HashMap<>(), sumSpread= new HashMap<>(),stderr = new HashMap<>(), relativeError  = new HashMap<>();
	public static HashMap<Long, Long> flowid = new HashMap<>(), elementid= new HashMap<>();
	public static HashMap<Long, HashSet<Long>> elements = new HashMap<>();
	public static boolean fullRandomID = false;
	public static   HashSet<Long> labels = new HashSet<>();

	/** parameters for count-min */
	public static final int d =4; 			// the number of rows in Count Min
	public static int w = 1;				// the number of columns in Count Min
	public static int u = 1;				// the size of each elementary data structure in Count Min.
	public static int[] S = new int[d];		// random seeds for Count Min
	public static int m = 1;				// number of bit/register in each unit (used for bitmap, FM sketch and HLL sketch)
	public static int repeat = 10, repeatTime = 100;
	public static HashMap<String, Double> spreads = new HashMap<>();
	public static int repeatRound = 0;
	/** parameters for counter */
	public static int mValueCounter = 1;			// only one counter in the counter data structure
	public static int counterSize = 32;				// size of each unit

	/** parameters for bitmap */
	public static final int bitArrayLength = 5000;
	
	/** parameters for FM sketch **/
	public static int mValueFM = 128;
	public static final int FMsketchSize = 32;
	
	/** parameters for HLL sketch **/
	public static int mValueHLL = 128;
	public static final int HLLSize = 4;
	public static int[] SHLL = new int[mValueHLL];		// random seeds for Count Min

	public static int times = 0;
	
	/** number of runs for throughput measurement */
	public static int loops = 200;
	
	public static void main(String[] args) throws FileNotFoundException {
		/** measurement for flow sizes **/
		
		System.out.println("Start****************************");
		/** measurement for flow sizes **/
		
		
		
		/** measurement for flow spreads **/
		
		loadStream();
		  for (repeatRound = 0; repeatRound<repeat; repeatRound++) {
			C = initCM();
			encodeSpread(GeneralUtil.dataStreamForFlowSpread);
    		estimateSpread(GeneralUtil.dataSummaryForFlowSpread);
		  }
		
		
		/** experiment for specific requirement *
		for (int i : expConfig) {
			switch (i) {
	        case 0:  initCM(0);
					 encodeSize(GeneralUtil.dataStreamForFlowSize);
					 randomEstimate(10000000);
	                 break;
	        default: break;
			}
		}*/
		System.out.println("DONE!****************************");
	}
	
	// Init the Count Min for different elementary data structures.
	public static GenrealHLL[][] initCM() {
		m = mValueHLL;
		u = HLLSize * mValueHLL;
		w = (M / u) / d;
		GenrealHLL[][] B = new GenrealHLL[d][w];
		Key = new long[d][w];
		Value = new int[d][w];
		for (int i = 0; i < d; i++) {
			for (int j = 0; j < w; j++) {
				B[i][j] = new GenrealHLL(mValueHLL, HLLSize, base);
			}
		}
		
		
		generateCMRamdonSeeds();
		return B;
		//System.out.println("\nCount Min-" + C[0][0].getDataStructureName() + " Initialized!");
	}
	
		
	// Generate random seeds for Counter Min.
	public static void generateCMRamdonSeeds() {
		HashSet<Integer> seeds = new HashSet<Integer>();
		int num = d;
		while (num > 0) {
			int s = rand.nextInt();
			if (!seeds.contains(s)) {
				num--;
				S[num] = s;
				seeds.add(s);
			}
		}
		num = mValueHLL;
		while (num > 0) {
			int s = rand.nextInt();
			if (!seeds.contains(s)) {
				num--;
				SHLL[num] = s;
				seeds.add(s);
			}
		}
	}

	
	
	/** Estimate flow sizes using random flow ids. */
	public static void increaseByOne(int cc, long curf, long fff) {
		if ( cc ==0) {
			curf = fff;
			cc += 1;
		}
		else if (curf == fff) {
			cc += 1;
		}
		else {
			double ppp = Math.pow(b, - cc);
			if (rand.nextFloat()<ppp) {
				curf = fff;
				cc -=1;
			}
		}
	}
	
	/** Encode elements to the Count Min for flow spread measurement. */
	public static void encodeSpread(String filePath) throws FileNotFoundException {
		System.out.println("Encoding elements using " + C[0][0].name.toUpperCase() + "s..." );
		Scanner sc = new Scanner(new File(filePath));
		n = 0;
		kkk =0;
		while (kkk++< pkt) {
			long flowid = flowida[kkk];
			long elementid = elementida[kkk];
			n++;
			for (int i = 0; i<d; i++) {
			
				int j = (GeneralUtil.intHash(GeneralUtil.FNVHash1((flowid)) ^ S[i]) % w + w) % w;
				double tempP = C[i][j].encode((flowid)^(elementid));
				double pCU =1/tempP / ((int)Math.ceil(1/tempP));
				if (rand.nextFloat()<pCU) {
					int tempVV = ((int)Math.ceil(1/tempP));
					while(tempVV-- > 0 ) {
						if ( Value[i][j] ==0) {
							Key[i][j] = flowid;
							Value[i][j] += 1;
						}
						else if (Key[i][j] == flowid) {
							Value[i][j] += 1;
						}
						else {
							double ppp = Math.pow(b, - Value[i][j]);
							if (rand.nextFloat()<ppp) {
								//Key[i][j] = flowid;
								Value[i][j] -= 1;
							}
						}
					}
				}
			}
			
		}
		System.out.println("Total number of encoded pakcets: " + n); 
		sc.close();
	}

	/** Estimate flow spreads. */
	public static void estimateSpread(String filepath) throws FileNotFoundException {
		
		System.out.println("Estimating Flow CARDINALITY... for "+repeatRound+"th times" ); 
		Scanner sc = new Scanner(new File(filepath));
		String resultFilePath = GeneralUtil.path + "countmin\\"
				+ "+-" + C[0][0].name
				+ "_M_" +  M / 1024 / 1024 + "_d_" + d + "_u_" + u + "_m_" + m;
		PrintWriter pw;
		if (repeatRound ==0) pw = new PrintWriter(new File(resultFilePath));
		else pw = new PrintWriter(new FileOutputStream(new File(resultFilePath),true));
		System.out.println("Result directory: " + resultFilePath); 
			
		long startTime = System.nanoTime();
				for(int i = 0; i < d; i++) {
					for (int ii = 0; ii < w; ii++) {
						long fff = Key[i][ii];
						int estimate = Value[i][ii];
						//System.out.print("--------"+fff);
						for(int iii = 0; iii < d; iii++) {
							int j = (GeneralUtil.intHash(GeneralUtil.FNVHash1(fff) ^ S[iii]) % w + w) % w;
							if (Key[iii][j] == fff) {
								estimate = Math.max(Value[iii][j], estimate);
							}
						}
						
					}
					//estimate = Math.min(estimate, C[i][j].getValue());
					
				}
				//estimate = (d%2)==1?value[(d-1)/2]:(value[d/2]+value[d/2-1])/2;
				//estimate =newEstimate;
				//estimate = Math.max(1, estimate);
				//spreads.put(flowid, newEstimate+spreads.getOrDefault(flowid, 0.0));
				long endTime = System.nanoTime();
				//System.out.println(realSpread.keySet().size());
				System.out.println("encodetime: " + (double)  (endTime - startTime) );	
		for (long fff:elements.keySet()) {
			int realS = elements.get(fff).size();
			int estimate = 0;
			for(int iii = 0; iii < d; iii++) {
				int j = (GeneralUtil.intHash(GeneralUtil.FNVHash1(fff) ^ S[iii]) % w + w) % w;
				if (Key[iii][j] == fff) {
					estimate = Math.max(Value[iii][j], estimate);
				}
			}
			if(realS>=SSthreshold) {
				superspreader++;
				//System.out.println("superspreader\t"+ superspreader+"\t"+ realS+"\t" + estimate);
				if (estimate<SSthreshold) {
					falsenegative ++;					
				}
			}
			if (estimate >= SSthreshold && realS <SSthreshold) {
				falsepositive ++;
			}
			if (estimate >= SSthreshold ) {
				SSReported++;
			}
		}
		if (repeatRound == repeat-1) {
			superspreader /= (double)repeat;
			falsenegative /= (double)repeat;
			falsepositive /= (double)repeat;
			SSReported /= (double)repeat;
			System.out.println(repeat+"---------");
			double precision = (superspreader-falsenegative)*1.0/SSReported, recall = (superspreader-falsenegative)*1.0/superspreader;
			double f1 = 2/(1/precision+1/recall);
			
			System.out.println("real super spreader:\t"+superspreader);
			System.out.println("false positive:\t"+falsepositive);
			System.out.println("false negative:\t"+falsenegative);
			System.out.println("f1\t"+f1 +"\t precision\t"+precision+"\t recall \t"+recall);

		}
		sc.close();
		pw.close();
		// obtain estimation accuracy results
		//if (repeatRound== repeat-1) GeneralUtil.analyzeAccuracy(resultFilePath);
	}
	
	public static void loadStream() throws FileNotFoundException {
		
		elements = new HashMap<>();
		
        Random ran1 = new Random();
        System.out.println("Processing source files...");
	 //For each 5-minute period
		HashMap<Long, HashSet<Long>> currentElements = new HashMap<>();
		Scanner sc = null;
		t = 0;
		try {
			String currentPath = GeneralUtil.dataStreamForFlowSpread;
			//System.out.println("Processing file:" + currentPath);
			sc = new Scanner(new File(currentPath));
			sc.nextLine();
			while (sc.hasNextLine()){
				String entry = sc.nextLine();
				if (entry.startsWith("Summary")) {
					sc.close();
					break;
				}
				String[] newline = entry.split("\\s+");
				flowida[pkt] = Long.parseLong(newline[1]); //Pass the source IP and source port (but then we keep only the source IP)
				elementida[pkt] = Long.parseLong(newline[0]); //Pass the source IP, source port and destination IP, destination port
				if (fullRandomID) {
					if (flowid.containsKey( flowida[pkt])) {
						flowida[pkt] = flowid.get(flowida[pkt]);
					}
					else {
						long randFlowid =  ran1.nextLong();
						while (flowid.containsKey(randFlowid)) {
							randFlowid =  ran1.nextLong();
						}
						flowid.put(flowida[pkt], randFlowid);
						flowida[pkt] = randFlowid;
					}
					if (elementid.containsKey( elementida[pkt])) {
						elementida[pkt] = elementid.get(elementida[pkt]);
					}
					else {
						long randElemid =  ran1.nextLong();
						while (elementid.containsKey(randElemid)) {
							randElemid =  ran1.nextLong();
						}
						elementid.put(elementida[pkt], randElemid);
						elementida[pkt] = randElemid;
					}
				
				
				}
				labels.add(flowida[pkt]);
				elements.putIfAbsent(flowida[pkt], new HashSet<>());
				if (!elements.get(flowida[pkt]).contains(elementida[pkt])) {
					elements.get(flowida[pkt]).add(elementida[pkt]);
				
					//pos[pkt] = t;
					pkt++;
					if (pkt % 100000 == 0) {
						System.out.println(pkt);
					}
				}
				t++;
			}
		}
		catch(IOException e) {
			e.printStackTrace();
		}
		finally {
			try {
				sc.close();
			}
			catch(Exception e) {
				e.printStackTrace();
			}
		}
	
		System.out.println("total number of packets "+t);
		System.out.println("total spread "+ pkt);
		
	}
	
}
