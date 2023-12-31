package bsktHLLbased;

import java.util.BitSet;
import java.util.HashSet;

public class FMsketch extends GeneralDataStructure {
	/** parameters for FM sketch */
	public static String name = "FMsketch";				// the data structure name
	public int FMsketchSize; 					// unit size for sketch
	public int m;								// number of sketches used
	public double phi = 0.77351;
	
	public BitSet[] FMsketchMatrix;
	
	public FMsketch(int m, int size) {
		super();
		FMsketchSize = size;
		this.m = m;
		FMsketchMatrix = new BitSet[m];
		for (int i = 0; i < m; i++) {
			FMsketchMatrix[i] = new BitSet(size);
		}
	}
	public BitSet[] getFM() {
		return FMsketchMatrix;
	}
	@Override
	public String getDataStructureName() {
		return name;
	}

	@Override

	public int getUnitSize() {
		return FMsketchSize*m;
	}
	
	@Override
	public int getValue() {
		return getValue(FMsketchMatrix);
	}
	
	@Override
	public void encode() {
		int r = rand.nextInt(Integer.MAX_VALUE);
		int k = r % m;
		int hash_val = GeneralUtil.intHash(r);
		int leadingZeros = Math.min(Integer.numberOfLeadingZeros(hash_val), FMsketchSize - 1);
		FMsketchMatrix[k].set(leadingZeros);
	}
	
	@Override
	public void encode(String elementID) {
		int hash_val = GeneralUtil.FNVHash1(elementID);
		int leadingZeros = Math.min(Integer.numberOfLeadingZeros(hash_val), FMsketchSize - 1);
		int k = (hash_val % m + m) % m;
		FMsketchMatrix[k].set(leadingZeros);
	}
	
	@Override
	public void encode(int elementID) {
		int hash_val = GeneralUtil.intHash(elementID);
		int leadingZeros = Math.min(Integer.numberOfLeadingZeros(hash_val), FMsketchSize - 1);
		int k = (hash_val % m + m) % m;
		FMsketchMatrix[k].set(leadingZeros);
	}
	
	public void encode(long elementID,int[] SFM) {
		int mValueFM = m;
		int hash_val = (GeneralUtil.intHash(GeneralUtil.FNVHash1(elementID))%mValueFM+mValueFM)%mValueFM;;
		int k = (GeneralUtil.intHash(GeneralUtil.FNVHash1(elementID) ^ SFM[hash_val]));
		int leadingZeros = Math.min(Integer.numberOfLeadingZeros(k), FMsketchSize - 1);
		FMsketchMatrix[hash_val].set(leadingZeros);
	}
	
	@Override
	public void encode(int flowID, int[] s) {
		int ms = s.length;
		int r = rand.nextInt(Integer.MAX_VALUE);
		int j = r % ms;
		int i = (GeneralUtil.intHash(flowID ^ s[j]) % m + m) % m;
		int hash_val = GeneralUtil.intHash(r);
		int leadingZeros = Math.min(Integer.numberOfLeadingZeros(hash_val), FMsketchSize - 1);
		FMsketchMatrix[i].set(leadingZeros);
	}
	public GeneralDataStructure[] getsplit(int m,int w) {
		GeneralDataStructure[]  B=new FMsketch[m];
		for(int i=0;i<m;i++) {
			B[i]=new FMsketch(w,w/m);
			for(int j=0;j<w/m;j++)
				B[i].getFMSketches()[j]=this.getFMSketches()[i*(w/m)+j];
		}
		return B;
	};
	@Override
	public void encodeSegment(String flowID, int[] s, int w) {
		int ms = s.length;
		int r = rand.nextInt(Integer.MAX_VALUE);
		int j = r % ms;
		int k = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % w + w) % w;
		int i = j * w + k;
		int hash_val = GeneralUtil.intHash(r);
		int leadingZeros = Math.min(Integer.numberOfLeadingZeros(hash_val), FMsketchSize - 1);
		FMsketchMatrix[i].set(leadingZeros);
	}
	public double encodeFM(String flowID, int[] s, String label) {
		double prob = 0.0;	
		if (label=="after") {
			for (int i=0;i<s.length;i++) {
			int k = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[i]) % m + m) % m;
			int hash_val = GeneralUtil.FNVHash1(flowID) ^ s[i];
			int leadingZeros = Math.min(Integer.numberOfLeadingZeros(hash_val), FMsketchSize - 1);
			if (!FMsketchMatrix[k].get(leadingZeros)) {prob +=1;FMsketchMatrix[k].set(leadingZeros);}
			}
		}
		else {
			for (int i=0;i<s.length;i++) {
			int k = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[i]) % m + m) % m;
			double probline =0.0;
				for (int j=0;j<FMsketchSize;j++) {
					if (!FMsketchMatrix[k].get(j)) probline += Math.pow(2,-1*(j+1));
				}
			prob += probline;
			}
		}
		return prob;
	}
	@Override
	public void encodeSegment(long flowID, int[] s, int w) {
		int ms = s.length;
		int r = rand.nextInt(Integer.MAX_VALUE);
		//GeneralUtil.set1.add(r);
		int j = r % ms;
		//long xx=(Long.parseLong("813931797")<<32)|Long.parseLong("3617472723");
			
		int k = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % w + w) % w;
		int i = j * w + k;
		int hash_val = GeneralUtil.intHash(r);
		//GeneralUtil.set2.add(hash_val);
		int leadingZeros = Math.min(Integer.numberOfLeadingZeros(hash_val), FMsketchSize - 1);
		FMsketchMatrix[i].set(leadingZeros);
		//if(xx==flowID){
		//	System.out.println(r+" "+k+" "+hash_val+" "+leadingZeros);
			
		//}
	}
	
	
	
	@Override
	public void encodeSegment(int flowID, int[] s, int w) {
		int ms = s.length;
		int r = rand.nextInt(Integer.MAX_VALUE);
		int j = r % ms;
		int k = (GeneralUtil.intHash(flowID ^ s[j]) % w + w) % w;
		int i = j * w + k;
		int hash_val = GeneralUtil.intHash(r);
		int leadingZeros = Math.min(Integer.numberOfLeadingZeros(hash_val), FMsketchSize - 1);
		FMsketchMatrix[i].set(leadingZeros);
	}
	
	@Override
	public void encode(String flowID, String elementID, int[] s) {
		int ms = s.length;
		int r = GeneralUtil.FNVHash1(elementID);
		int j = (r % ms+ms)%ms;
		int i = GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) ;
		boolean isMinus = Integer.numberOfLeadingZeros(i)>=1;
		if (isMinus) j+=(m)/2;
		int hash_val = GeneralUtil.FNVHash1(String.valueOf(r));
		int leadingZeros = Math.min(Integer.numberOfLeadingZeros(hash_val), FMsketchSize - 1);
		FMsketchMatrix[j].set(leadingZeros);
	}
	
	@Override
	public void encode(long flowID, long elementID, int[] s) {
		int ms = s.length;
		int j = (GeneralUtil.FNVHash1(elementID) % ms + ms) % ms;
		int i = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % m + m) % m;
		int hash_val = GeneralUtil.FNVHash1(elementID);
		int leadingZeros = Math.min(Integer.numberOfLeadingZeros(hash_val), FMsketchSize - 1);
		FMsketchMatrix[i].set(leadingZeros);
	}
	@Override
	public void encodeSegment(long flowID, long elementID, int[] s, int w) {
		int ms = s.length;
		int j = ((GeneralUtil.FNVHash1(flowID^elementID)) % ms + ms) % ms;
		int temp = GeneralUtil.FNVHash1(flowID^s[j]);
		boolean isMinus = Integer.numberOfLeadingZeros(temp)>=1;
		int k = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % (w/2) + (w/2)) % (w/2);
		int i = j * w + k;
		if (isMinus) i+=w/2;
		int hash_val = GeneralUtil.FNVHash1(elementID);
		int leadingZeros = Math.min(Integer.numberOfLeadingZeros(hash_val), FMsketchSize - 1);
		FMsketchMatrix[i].set(leadingZeros);
	}

	@Override
	public void encodeSegment(String flowID, String elementID, int[] s, int w) {
		int m = s.length;
		int j = (GeneralUtil.FNVHash1(elementID) % m + m) % m;
		int k = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % w + w) % w;
		int i = j * w + k;
		int hash_val = GeneralUtil.FNVHash1(elementID);
		int leadingZeros = Math.min(Integer.numberOfLeadingZeros(hash_val), FMsketchSize - 1);
		FMsketchMatrix[i].set(leadingZeros);
	}
	
	@Override
	public void encodeSegment(int flowID, int elementID, int[] s, int w) {
		int m = s.length;
		int j = (GeneralUtil.intHash(elementID) % m + m) % m;
		int k = (GeneralUtil.intHash(flowID ^ s[j]) % w + w) % w;
		int i = j * w + k;
		int hash_val = GeneralUtil.intHash(elementID);
		int leadingZeros = Math.min(Integer.numberOfLeadingZeros(hash_val), FMsketchSize - 1);
		FMsketchMatrix[i].set(leadingZeros);
	}
	
	@Override
	public void encode(int flowID, int elementID, int[] s) {
		int ms = s.length;
		int hash_val = GeneralUtil.intHash(elementID);
		int j = (hash_val% ms + ms) % ms;
		int i = (GeneralUtil.intHash(flowID ^ s[j]) % m + m) % m;
		int leadingZeros = Math.min(Integer.numberOfLeadingZeros(hash_val), FMsketchSize - 1);
		FMsketchMatrix[i].set(leadingZeros);
	}
	
	@Override
	public void encode(String flowID, int[] s) {
		int ms = s.length;
		int r = rand.nextInt(Integer.MAX_VALUE);
		int j = r % ms;
		int i = GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) ;
		boolean isMinus = Integer.numberOfLeadingZeros(i)>=1;
		if (isMinus) j+=(m)/2;
		int hash_val = GeneralUtil.FNVHash1(String.valueOf(r));
		int leadingZeros = Math.min(Integer.numberOfLeadingZeros(hash_val), FMsketchSize - 1);
		FMsketchMatrix[j].set(leadingZeros);
	}
	@Override

	public int getValue(String flowID, int[] s) {
		int ms = s.length;
		BitSet[] sketch = new BitSet[ms];
		BitSet[] sketchMinus = new BitSet[ms];

		for (int j = 0; j < ms; j++) {
			int i = GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) ;
			boolean isMinus = Integer.numberOfLeadingZeros(i)>=1;
			sketch[j] = FMsketchMatrix[j];			
			if (isMinus) {sketch[j] =FMsketchMatrix[j+m/2];sketchMinus[j] = FMsketchMatrix[j];}
			else {sketch[j] =FMsketchMatrix[j];sketchMinus[j] = FMsketchMatrix[j+m/2];}


		}
		return getValue(sketch)-getValue(sketchMinus);
	}
	
	@Override
	public int getValue(long flowID, int[] s) {
		int ms = s.length;
		BitSet[] sketch = new BitSet[ms];
		for (int j = 0; j < ms; j++) {
			int i = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % m + m) % m;
			sketch[j] = FMsketchMatrix[i];
		}
		return getValue(sketch);
	}
	
	@Override
	public int getValueSegment(String flowID, int[] s, int w) {
		int ms = s.length;
		BitSet[] sketch = new BitSet[ms];
		for (int j = 0; j < ms; j++) {
			int i = j * w + (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % w + w) % w;
			sketch[j] = FMsketchMatrix[i];
		}
		return getValue(sketch);
	}
	
	@Override
	public int getValueSegment(long flowID, int[] s, int w) {
		int ms = s.length;
		BitSet[] sketch = new BitSet[ms];
		BitSet[] sketchMinus = new BitSet[ms];

		for (int j = 0; j < ms; j++) {
			int temp = GeneralUtil.FNVHash1(flowID^s[j]);
			boolean minus = Integer.numberOfLeadingZeros(temp)>=1;
			int i = j * w + (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % (w/2) + (w/2)) % (w/2);
			if (!minus) {sketch[j] = FMsketchMatrix[i];i+=w/2;sketchMinus[j]=FMsketchMatrix[i];}
			else {sketchMinus[j] = FMsketchMatrix[i];i+=w/2;sketch[j]=FMsketchMatrix[i];}	

			//sketch[j] = FMsketchMatrix[i];
		}
		return getValue(sketch)-getValue(sketchMinus);
	}
	
	/**
	 * reduce the number of estimator used to reduce bias for small flows.
	 */
	@Override
	public int getOptValueSegment(String flowID, int[] s, int w, int sample_ratio) {
		int ms = s.length;
		int[] indexes = new int[ms];
		for (int j = 0; j < ms; j++) {
			int i = j * w + (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % w + w) % w;
			indexes[j] = i;
		}
		int mss = ms / sample_ratio, i = 0;
		BitSet[] sketch_sampled = new BitSet[mss];
		HashSet<Integer> index_sampled = new HashSet<>();
		while (index_sampled.size() < mss) {
			if (rand.nextDouble() < 1.0 / sample_ratio) {
				index_sampled.add(indexes[i]);
				//System.out.println("Sampled!");
			}
			i = (i + 1) % ms;
		}
		int j = 0;
		for (int k: index_sampled) {
			sketch_sampled[j++] = FMsketchMatrix[k];
		}
		//System.out.println("Sampled virtual sketch size: " + index_sampled.size() + "\nOriginal virtual sketch size: " + ms);
		return getValue(sketch_sampled);
	}
	
	public int getValue(BitSet[] fm) {
		int len = fm.length;
		Double result = 0.0;
		double fillRate = 0; //fill rate of zero values.
		int Z_S = 0;
		for (int i = 0; i < len; i++) {
			if (fm[i].cardinality() == 0) fillRate += 1.0 / len;
			Z_S = Z_S + fm[i].nextClearBit(0);		// next zero bit from start position
		}
		
		result = len * Math.pow(2,  1.0 * Z_S / len) /phi;///getPhi(fillRate); //phi
		return result.intValue();
	}
	
	// parameters to get phi.
	public static final double bounds[] = {0, 0.07369180869234335, 0.10262304342166248, 0.20981173432019362, 0.5, 0.5816728608049805, 0.7046534091884438, 0.7359343526826588, 0.7787836476835744, 0.791859320637246, 0.7956275178538729, 0.8129609277163979, 0.8471668287937744,  0.8833217592592593, 0.9480086976424811, 1};
	public static double values[] = {1, 1.08365367995234903, 1.13598645486990289, 1.28242386482358365, 2.371794872, 3.23166647, 6.5237687261840187, 8.611238191, 15.2585804, 16.091321328, 21.136421074, 26.894900035485, 64.992943221, 242.64121466, 9416.397404917, 10000};	
	
	public double getPhi(double p) {
		return phi * biasCorrectionbyFillRateTo(p);
	}
	
	private double biasCorrectionbyFillRateTo(double p_fill_rate) {   
    	for(int i=0; i < bounds.length; i++) {
        	if(compareTwoDoubles(p_fill_rate, bounds[i]) == 0)
        		return values[i];
        	
        	if(i != bounds.length - 1) {
	        	double left=bounds[i], right=bounds[i+1];
	        	if(compareTwoDoubles(p_fill_rate, left) > 0 
	        	&& compareTwoDoubles(p_fill_rate, right) < 0)
	    			return linearDerivation(
	    					left, values[i], 
	    					right, values[i+1], 
	    					p_fill_rate);
        	}
    	}
    	return values[values.length-1];
	}
	
    private double linearDerivation(double x1, double y1, double x2, double y2, double x3) {
    	return ((x3 - x1) * y2 - (x3 - x2) * y1) / (x2 - x1); 
    }
	   
    private int compareTwoDoubles(double x1, double x2) {
    	final double error = 1e-3;
    	if(Double.compare(x1, x2-error) < 0)
    		return -1;

    	if(Double.compare(x1, x2+error) > 0)
    		return 1;
    	
    	return 0;
    }

	@Override
	public int[] getCounters() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BitSet getBitmaps() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BitSet[] getFMSketches() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BitSet[] getHLLs() {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public GeneralDataStructure join(GeneralDataStructure gds,int w,int i) {
		FMsketch fm = (FMsketch) gds;
		for (int j = 0; j < w; j++) {
			FMsketchMatrix[i].or(fm.FMsketchMatrix[i*w+j]);
		}
		return this;
	}
	@Override
	public GeneralDataStructure join(GeneralDataStructure gds) {
		FMsketch fm = (FMsketch) gds;
		for (int i = 0; i < m; i++) {
			FMsketchMatrix[i].or(fm.FMsketchMatrix[i]);
		}
		return this;
	}

	@Override
	public void encodeSegment(int flowid, int[] s, int w, int subFlowSize) {
		// TODO Auto-generated method stub
		
	}
	@Override
	protected void setSingleCounter(int min) {
		// TODO Auto-generated method stub
		
	}
	@Override
	protected void setBitSetValue(int i, int max) {
		// TODO Auto-generated method stub
		
	}
	@Override
	protected BitSet[] getMinSegment(String flowid, int[] is, int i, BitSet[] temp) {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	protected BitSet[] getMaxSegment(String flowid, int[] is, int i, BitSet[] temp) {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	protected BitSet[] getMinSegment(BitSet[] temp, BitSet[] temp1) {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	protected int getBitSetValue(BitSet bitSet) {
		// TODO Auto-generated method stub
		return 0;
	}
	@Override
	protected BitSet[] getMaxSegment(BitSet[] temp, BitSet[] temp1) {
		// TODO Auto-generated method stub
		return null;
	}
}
