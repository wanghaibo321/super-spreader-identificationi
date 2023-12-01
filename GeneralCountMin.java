package bsktHLLbased;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
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

public class GeneralCountMin {
	public static Random rand = new Random();
	
	public static int n = 0; 						// total number of packets
	public static int flows = 0; 					// total number of flows
	public static int avgAccess = 0; 				// average memory access for each packet
	public static final int M = 1024 * 2048; 	// total memory space Mbits	
	public static GeneralDataStructure[][] C;
	public static Set<Integer> sizeMeasurementConfig = new HashSet<>(Arrays.asList()); // -1-regular CM; 0-enhanced CM; 1-Bitmap; 2-FM sketch; 3-HLL sketch
	public static Set<Integer> spreadMeasurementConfig = new HashSet<>(Arrays.asList(1,3)); // 1-Bitmap; 2-FM sketch; 3-HLL sketch
	public static Set<Integer> expConfig = new HashSet<>(Arrays.asList()); //0-ECountMin dist exp
	public static boolean isGetThroughput = false;
	
	/** parameters for count-min */
	public static final int d =4; 			// the number of rows in Count Min
	public static int w = 1;				// the number of columns in Count Min
	public static int u = 1;				// the size of each elementary data structure in Count Min.
	public static int[] S = new int[d];		// random seeds for Count Min
	public static int m = 1;				// number of bit/register in each unit (used for bitmap, FM sketch and HLL sketch)
	public static int repeat = 1;
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
	public static final int HLLSize = 5;
	public static int[] SHLL = new int[mValueHLL];		// random seeds for Count Min

	public static int times = 0;
	
	/** number of runs for throughput measurement */
	public static int loops = 200;
	
	public static void main(String[] args) throws FileNotFoundException {
		/** measurement for flow sizes **/
		if (isGetThroughput) {
			getThroughput();
			return;
		}
		System.out.println("Start****************************");
		/** measurement for flow sizes **/
		
		for (int i : sizeMeasurementConfig) {
			times = 0;
			for(int j = 0; j < 1; j++) {
				initCM(i);
				//getThroughput();
				encodeSize(GeneralUtil.dataStreamForFlowSize);
				//long endTime = System.nanoTime();
				//double duration = 1.0 * (endTime - startTime) / 1000000000;
				//System.out.println("Average execution time: " + 1.0 * duration / loops + " seconds");
				//System.out.println("Average Throughput: " + 1.0 * n / (duration / loops) + " packets/second" );
	        	estimateSize(GeneralUtil.dataSummaryForFlowSize);
	        	times++;
			}
		}
		
		/** measurement for flow spreads **/
		
		for (int i : spreadMeasurementConfig) {
		  for (repeatRound = 0; repeatRound<repeat; repeatRound++) {
			initCM(i);
			encodeSpread(GeneralUtil.dataStreamForFlowSpread);
    		estimateSpread(GeneralUtil.dataSummaryForFlowSpread);
		  }
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
	public static void initCM(int index) {
		switch (index) {
	        case 0: case -1: C = generateCounter();
	                 break;
	        case 1:  C = generateBitmap();
	                 break;
	        case 2:  C = generateFMsketch();
	                 break;
	        case 3:  C = generateHyperLogLog();
	                 break;
	        default: break;
		}
		generateCMRamdonSeeds();
		//System.out.println("\nCount Min-" + C[0][0].getDataStructureName() + " Initialized!");
	}
	
	// Generate counter base Counter Min for flow size measurement.
	public static Counter[][] generateCounter() {
		m = mValueCounter;
		u = counterSize * mValueCounter;
		w = (M / u) / d;
		Counter[][] B = new Counter[d][w];
		for (int i = 0; i < d; i++) {
			for (int j = 0; j < w; j++) {
				B[i][j] = new Counter(1, counterSize);
			}
		}
		return B;
	}
	
	// Generate bitmap base Counter Min for flow cardinality measurement.
	public static Bitmap[][] generateBitmap() {
		m = bitArrayLength;
		u = bitArrayLength;
		w = (M / u) / d;
		Bitmap[][] B = new Bitmap[d][w];
		for (int i = 0; i < d; i++) {
			for (int j = 0; j < w; j++) {
				B[i][j] = new Bitmap(bitArrayLength);
			}
		}
		return B;
	}
	
	// Generate FM sketch base Counter Min for flow cardinality measurement.
	public static FMsketch[][] generateFMsketch() {
		m = mValueFM;
		u = FMsketchSize * mValueFM;
		w = (M / u) / d;
		FMsketch[][] B = new FMsketch[d][w];
		for (int i = 0; i < d; i++) {
			for (int j = 0; j < w; j++) {
				B[i][j] = new FMsketch(mValueFM, FMsketchSize);
			}
		}
		return B;
	}
	
	// Generate HLL sketch base Counter Min for flow cardinality measurement.
	public static HyperLogLog[][] generateHyperLogLog() {
		m = mValueHLL;
		u = HLLSize * mValueHLL;
		w = (M / u) / d;
		HyperLogLog[][] B = new HyperLogLog[d][w];
		for (int i = 0; i < d; i++) {
			for (int j = 0; j < w; j++) {
				B[i][j] = new HyperLogLog(mValueHLL, HLLSize);
			}
		}
		return B;
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

	/** Encode elements to the Count Min for flow size measurement. */
	public static void encodeSize(String filePath) throws FileNotFoundException {
		System.out.println("Encoding elements using " + C[0][0].getDataStructureName().toUpperCase() + "s..." );
		Scanner sc = new Scanner(new File(filePath));
		n = 0;
		
		while (sc.hasNextLine()) {
			String entry = sc.nextLine();
			String[] strs = entry.split("\\s+");
			int[] hashIndex = new int[d];
			int[] hashValue = new int[d];
			String flowid = strs[1];//GeneralUtil.getSizeFlowID(strs, true);
			n++;
			
			if (C[0][0].getDataStructureName().equals("Counter")) {
				int minVal = Integer.MAX_VALUE;
                for (int i = 0; i < d; i++) {
                    int j = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowid) ^ S[i]) % w + w) % w;
                    hashIndex[i] = j;                    
                    minVal = Math.min(minVal, C[i][j].getValue());
                }
                for (int i = 0; i < d; i++) {
		            int j = hashIndex[i];
		            if (C[i][j].getValue() == minVal) {
		            	C[i][j].encode();           
		            }
                }
			} 
		}
		System.out.println("Total number of encoded pakcets: " + n);
		sc.close();
	}

	/** Estimate flow sizes. */
	public static void estimateSize(String filePath) throws FileNotFoundException {
		System.out.println("Estimating Flow SIZEs..." ); 
		Scanner sc = new Scanner(new File(filePath));
		String resultFilePath = GeneralUtil.path + "countmin\\CM_" + C[0][0].getDataStructureName()
				+ "_M_" +  M / 1024  + "_d_" + d + "_u_" + u + "_m_" + m + "_T_" + times;
		PrintWriter pw = new PrintWriter(new File(resultFilePath));
		System.out.println("Result directory: " + resultFilePath); 
		while (sc.hasNextLine()) {
			String entry = sc.nextLine();
			String[] strs = entry.split("\\s+");
			String flowid = strs[0];//GeneralUtil.getSizeFlowID(strs, false);
			int num = Integer.parseInt(strs[strs.length-1]);
			if (true) {
				int estimate = Integer.MAX_VALUE;
				
				for(int i = 0; i < d; i++) {
					int j = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowid) ^ S[i]) % w + w) % w;
					int value= C[i][j].getValue();
					if (value<estimate)
						estimate = value;
					//estimate = Math.min(estimate, C[i][j].getValue());
				}
				if (estimate<1) estimate = 1;
				pw.println(entry + "\t" + estimate);
			}
		}
		sc.close();
		pw.close();
		// obtain estimation accuracy results
		GeneralUtil.analyzeAccuracy(resultFilePath);
	}
	
	/** Estimate flow sizes using random flow ids. */
	public static void randomEstimate(int numOfFlows) throws FileNotFoundException {
		System.out.println("Estimating Flow SIZEs..." ); 
		String resultFilePath = GeneralUtil.path + "Random\\+-" + C[0][0].getDataStructureName()
				+ "_M_" +  M / 1024 / 1024 + "_d_" + d + "_u_" + u + "_m_" + m;
		PrintWriter pw = new PrintWriter(new File(resultFilePath));
		System.out.println("Result directory: " + resultFilePath); 
		for (int times = 0; times < numOfFlows; times++) {
			String flowid = String.valueOf(rand.nextDouble());
			int estimate = Integer.MAX_VALUE;
			
			for(int i = 0; i < d; i++) {
				int j = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowid) ^ S[i]) % w + w) % w;
				estimate = Math.min(estimate, C[i][j].getValue());
			}
			pw.println(flowid + "\t" + estimate);
		}
		pw.close();
	}
	
	/** Encode elements to the Count Min for flow spread measurement. */
	public static void encodeSpread(String filePath) throws FileNotFoundException {
		System.out.println("Encoding elements using " + C[0][0].getDataStructureName().toUpperCase() + "s..." );
		Scanner sc = new Scanner(new File(filePath));
		n = 0;
		while (sc.hasNextLine()) {
			String entry = sc.nextLine();
			String[] strs = entry.split("\\s+");
			String[] res = GeneralUtil.getSperadFlowIDAndElementID(strs, true);
			String flowid = res[0];
			String elementid = res[1];
			n++;
			for (int i = 0; i<d; i++) {
			
				int j = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowid) ^ S[i]) % w + w) % w;
				C[i][j].encode(Long.parseLong(flowid),Long.parseLong(elementid),SHLL);
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
				+ "+-" + C[0][0].getDataStructureName()
				+ "_M_" +  M / 1024 / 1024 + "_d_" + d + "_u_" + u + "_m_" + m;
		PrintWriter pw;
		if (repeatRound ==0) pw = new PrintWriter(new File(resultFilePath));
		else pw = new PrintWriter(new FileOutputStream(new File(resultFilePath),true));
		System.out.println("Result directory: " + resultFilePath); 
		while (sc.hasNextLine()) {
			String entry = sc.nextLine();
			String[] strs = entry.split("\\s+");
			String flowid = GeneralUtil.getSperadFlowIDAndElementID(strs, false)[0];
			int num = Integer.parseInt(strs[strs.length-1]);
			// TODO(youzhou): Add sampling mechanism to reduce the computation time.
			if (rand.nextDouble() <= GeneralUtil.getSpreadSampleRate(num)) {
				int estimate = Integer.MAX_VALUE;
				int [] value = new int[d];
				int newEstimate =  0;
				for(int i = 0; i < d; i++) {
					int j = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowid) ^ S[i]) % w + w) % w;
					//estimate = Math.min(estimate, C[i][j].getValue());
					value[i]  = C[i][j].getValue();
					if (estimate > value[i])
						estimate = value[i];
				}
				Arrays.sort(value);
				//estimate = (d%2)==1?value[(d-1)/2]:(value[d/2]+value[d/2-1])/2;
				//estimate =newEstimate;
				//estimate = Math.max(1, estimate);
				//spreads.put(flowid, newEstimate+spreads.getOrDefault(flowid, 0.0));
				
				pw.println(entry + "\t" + estimate);
			}
		}
		sc.close();
		pw.close();
		// obtain estimation accuracy results
		if (repeatRound== repeat-1) GeneralUtil.analyzeAccuracy(resultFilePath);
	}
	
	public static void getThroughput() throws FileNotFoundException {
		Scanner sc = new Scanner(new File(GeneralUtil.dataStreamForFlowThroughput));
		ArrayList<Integer> dataFlowID = new ArrayList<Integer> ();
		ArrayList<Integer> dataElemID = new ArrayList<Integer> ();
		n = 0;
		if (sizeMeasurementConfig.size() > 0) {
			while (sc.hasNextLine()) {
				String entry = sc.nextLine();				
				String[] strs = entry.split("\\s+");
				dataFlowID.add(GeneralUtil.FNVHash1(entry));
				dataElemID.add(GeneralUtil.FNVHash1(strs[1]));
				n++;
			}
			sc.close();
		} else {
			while (sc.hasNextLine()) {
				String entry = sc.nextLine();				
				String[] strs = entry.split("\\s+");
				dataFlowID.add(GeneralUtil.FNVHash1(strs[0]));
				dataElemID.add(GeneralUtil.FNVHash1(strs[1]));
				n++;
			}
			sc.close();
		}
		System.out.println("total number of packets: " + n);
		
		/** measurment for flow sizes **/
		for (int i : sizeMeasurementConfig) {
			tpForSize(i, dataFlowID, dataElemID);
		}
		
		/** measurment for flow spreads **/
		for (int i : spreadMeasurementConfig) {
			tpForSpread(i, dataFlowID, dataElemID);
		}
	}
	
	/** Get throughput for flow size measurement. */
	public static void tpForSize(int sketchMin, ArrayList<Integer> dataFlowID, ArrayList<Integer> dataElemID) throws FileNotFoundException {
		int totalNum = dataFlowID.size();
		initCM(sketchMin);
		String resultFilePath = GeneralUtil.path + "Throughput\\CM_size_" + C[0][0].getDataStructureName()
				+ "_M_" +  M / 1024 / 1024 + "_d_" + d + "_u_" + u + "_m_" + m + "_tp_" + GeneralUtil.throughputSamplingRate;
		PrintWriter pw = new PrintWriter(new File(resultFilePath));
		Double res = 0.0;
		
		if (sketchMin == 0) { // for enhanced countmin
			double duration = 0;
			
			for (int i = 0; i < loops; i++) {
				initCM(sketchMin);
				int[] arrIndex = new int[d];
				int[] arrVal = new int[d];
				
				long startTime = System.nanoTime();
				for (int j = 0; j < totalNum; j++) {
					//if (rand.nextDouble() <= GeneralUtil.throughputSamplingRate) {
						int minVal = Integer.MAX_VALUE;
		                for (int k = 0; k < d; k++) {
		                    int jj = (GeneralUtil.intHash(dataFlowID.get(j) ^ S[k]) % w + w) % w;
		                    arrIndex[k] = jj;
		                    arrVal[k] = C[k][jj].getValue();
		                    minVal = Math.min(minVal, arrVal[k]);
		                }
		                
		                for (int k = 0; k < d; k++) {
				            if (arrVal[k] == minVal) {
				            	C[k][arrIndex[k]].encode(dataElemID.get(j));           
				            }
		                }
					//}
				}
				long endTime = System.nanoTime();
				duration += 1.0 * (endTime - startTime) / 1000000000;
			}
			res = 1.0 * totalNum / (duration / loops);
			//System.out.println("Average execution time: " + 1.0 * duration / loops + " seconds");
			System.out.println(C[0][0].getDataStructureName() + "\t Average Throughput: " + 1.0 * totalNum / (duration / loops) + " packets/second" );
		} else {
			double duration = 0;

			
			for (int i = 0; i < loops; i++) {
				initCM(sketchMin);
				long startTime = System.nanoTime();
				for (int j = 0; j < totalNum; j++) {
					//if (rand.nextDouble() <= GeneralUtil.throughputSamplingRate) {
		                for (int k = 0; k < d; k++) {
		                	C[k][(GeneralUtil.intHash(dataFlowID.get(j) ^ S[k]) % w + w) % w].encode();
		                }
					//}
				}
				long endTime = System.nanoTime();
				duration += 1.0 * (endTime - startTime) / 1000000000;
			}
			
			res = 1.0 * totalNum / (duration / loops);
			//System.out.println("Average execution time: " + 1.0 * duration / loops + " seconds");
			System.out.println(C[0][0].getDataStructureName() + "\t Average Throughput: " + 1.0 * totalNum / (duration / loops) + " packets/second" );
		}
		pw.println(res.intValue());
		pw.close();
	}
	
		/** Get throughput for flow spread measurement. */
	public static void tpForSpread(int sketchMin, ArrayList<Integer> dataFlowID, ArrayList<Integer> dataElemID) throws FileNotFoundException {
		int totalNum = dataFlowID.size();
		initCM(sketchMin);
		String resultFilePath = GeneralUtil.path + "Throughput\\CM_spread_" + C[0][0].getDataStructureName()
				+ "_M_" +  M / 1024 / 1024 + "_d_" + d + "_u_" + u + "_m_" + m + "_tp_" + GeneralUtil.throughputSamplingRate;
		PrintWriter pw = new PrintWriter(new File(resultFilePath));
		Double res = 0.0;
		
		double duration = 0;

		for (int i = 0; i < loops; i++) {
			initCM(sketchMin);
			long startTime = System.nanoTime();
			for (int j = 0; j < totalNum; j++) {
				for (int k = 0; k < d; k++) {
                	C[k][(GeneralUtil.intHash(dataFlowID.get(j) ^ S[k]) % w + w) % w].encode(dataElemID.get(j));
                }
			}	
			long endTime = System.nanoTime();
			duration += 1.0 * (endTime - startTime) / 1000000000;
		}
		res = 1.0 * totalNum / (duration / loops);
		//System.out.println("Average execution time: " + 1.0 * duration / loops + " seconds");
		System.out.println(C[0][0].getDataStructureName() + "\t Average Throughput: " + 1.0 * totalNum / (duration / loops) + " packets/second" );
		pw.println(res.intValue());
		pw.close();
	}
}
