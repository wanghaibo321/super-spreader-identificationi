package bsktHLLbased;

import java.util.BitSet;
import java.util.HashSet;
import java.util.Random;

public class HyperLogLog extends GeneralDataStructure {
	/** parameters for HLL */
	public String name = "HyperLogLog";				// the data structure name
	public int HLLSize; 								// unit size for each register
	public int m;									// the number of registers in the HLL sketch
	public int maxRegisterValue;
	public double alpha;
	//public int[] HLL;
	public BitSet[] HLLMatrix;
	
	public HyperLogLog(int m, int size) {
		super();
		HLLSize = size;
		this.m = m;
		maxRegisterValue = (int) (Math.pow(2, size) - 1);
		HLLMatrix = new BitSet[m];
		for (int i = 0; i < m; i++) {
			HLLMatrix[i] = new BitSet(size);
		}
		alpha = getAlpha(m);
	}

	@Override
	public String getDataStructureName() {
		return name;
	}
	
	@Override
	public BitSet getBitmaps() {
		return null;
	}
	
	@Override
	public int[] getCounters() {
		return null;
	};
	
	@Override
	public BitSet[] getFMSketches() {
		return null;
	};
	
	@Override
	public BitSet[] getHLLs() {
		return HLLMatrix;
	};

	@Override
	public int getUnitSize() {
		return HLLSize*m;
	}

	@Override
	public int getValue() {
		return getValue(HLLMatrix);
	}
	@Override
	public GeneralDataStructure[] getsplit(int m,int w) {
		GeneralDataStructure[]  B=new HyperLogLog[m];
		for(int i=0;i<m;i++) {
			B[i]=new HyperLogLog(w,5);
			for(int j=0;j<w/m;j++)
				B[i].getHLLs()[j]=this.HLLMatrix[i*(w/m)+j];
		}
		return B;
	}
	@Override
	public void encode() {
		int r = rand.nextInt(Integer.MAX_VALUE);
		int k = r % m;
		int hash_val = GeneralUtil.intHash(r);
		int leadingZeros = Integer.numberOfLeadingZeros(hash_val) + 1; // % hyperLogLog_size + hyperLogLog_size) % hyperLogLog_size;
		leadingZeros = Math.min(leadingZeros, maxRegisterValue);
		if (getBitSetValue(HLLMatrix[k]) < leadingZeros) {
			setBitSetValue(k, leadingZeros);
		}
	}
	
	@Override
	public void encode(String elementID) {
		int hash_val = GeneralUtil.FNVHash1(elementID);
		boolean isMinus = Integer.numberOfLeadingZeros(hash_val)>=1;
		hash_val = hash_val<<1;
		int leadingZeros = Integer.numberOfLeadingZeros(hash_val) + 1; // % hyperLogLog_size + hyperLogLog_size) % hyperLogLog_size;
		int k = (hash_val % (m/2) + (m/2)) % (m/2);
		if (isMinus) k +=m/2;
		leadingZeros = Math.min(leadingZeros, maxRegisterValue);
		if (getBitSetValue(HLLMatrix[k]) < leadingZeros) {
			setBitSetValue(k, leadingZeros);
		}
	}
	
	@Override
	public void encode(int elementID) {
 		int hash_val = GeneralUtil.intHash(elementID);
		int leadingZeros = Integer.numberOfLeadingZeros(hash_val) + 1; // % hyperLogLog_size + hyperLogLog_size) % hyperLogLog_size;
		int k = (hash_val % m + m) % m;
		leadingZeros = Math.min(leadingZeros, maxRegisterValue);
		if (getBitSetValue(HLLMatrix[k]) < leadingZeros) {
			setBitSetValue(k, leadingZeros);
		}
	}
	

	
	
	@Override
	public void encode(int flowID, int[] s) {
		int r = rand.nextInt(Integer.MAX_VALUE);
		int j = r % s.length;
		int i = (GeneralUtil.intHash(flowID ^ s[j]) % m + m) % m;
		int hash_val = GeneralUtil.intHash(r);
		int leadingZeros = Integer.numberOfLeadingZeros(hash_val) + 1;
		leadingZeros = Math.min(leadingZeros, maxRegisterValue);
		if (getBitSetValue(HLLMatrix[i]) < leadingZeros) {
			setBitSetValue(i, leadingZeros);
		}
	}
	public void encode(long elementID, int[] SHLL) {
		int mValueFM = m;
		int hash_val = (GeneralUtil.intHash(GeneralUtil.FNVHash1(elementID))%mValueFM+mValueFM)%mValueFM;
		//System.out.println(hash_val);
		int k = (GeneralUtil.intHash(GeneralUtil.FNVHash1(elementID) ^ SHLL[hash_val]));
		int leadingZeros = Math.min(Integer.numberOfLeadingZeros(k)+1, maxRegisterValue);
		//leadingZeros = Integer.numberOfLeadingZeros(hash_val) + 1;
		//if (leadingZeros>=3) System.out.println(leadingZeros+":"+k);
		//leadingZeros = Math.min(leadingZeros, maxRegisterValue);
		if (getBitSetValue(HLLMatrix[hash_val]) < leadingZeros) {
			setBitSetValue(hash_val, leadingZeros);
		}
	}
	@Override
	public void encode(long flowID, long elementID, int[] s) {
		int ms = s.length;
		int j = (GeneralUtil.FNVHash1(elementID) % ms + ms) % ms;
		int i = (GeneralUtil.intHash(GeneralUtil.FNVHash1(elementID) ^ s[0]) % ms + ms) % ms;
		int hash_val = GeneralUtil.FNVHash1(elementID);
		int leadingZeros = Integer.numberOfLeadingZeros(hash_val) + 1;
		leadingZeros = Math.min(leadingZeros, maxRegisterValue);
		if (getBitSetValue(HLLMatrix[i]) < leadingZeros) {
			setBitSetValue(i, leadingZeros);
		}
	}
	
	@Override
	public void encode(String flowID, String elementID, int[] s) {
		int r = GeneralUtil.FNVHash1(flowID + elementID);
		int ms = s.length;
		int j = (r % ms+ms)%ms;
		int i = GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]);
		boolean isMinus = Integer.numberOfLeadingZeros(i)>=1;
		j = (j % (m/2) + (m/2)) % (m/2);

		if (isMinus) j+=m/2;
		int hash_val = GeneralUtil.FNVHash1(r^ s[0]);//9304 for better prformance
		int leadingZeros = Integer.numberOfLeadingZeros(hash_val) + 1;
		leadingZeros = Math.min(leadingZeros, maxRegisterValue);
		if (getBitSetValue(HLLMatrix[j]) < leadingZeros) {
			setBitSetValue(j, leadingZeros);
		}
	}
	
	@Override
	public void encode(int flowID, int elementID, int[] s) {
		int ms = s.length;
		int hash_val = GeneralUtil.intHash(elementID);
		int j = (hash_val % ms + ms) % ms;
		int i = (GeneralUtil.intHash(flowID ^ s[j]) % m + m) % m;
		int leadingZeros = Integer.numberOfLeadingZeros(hash_val) + 1;
		leadingZeros = Math.min(leadingZeros, maxRegisterValue);
		if (getBitSetValue(HLLMatrix[i]) < leadingZeros) {
			setBitSetValue(i, leadingZeros);
		}
	}
	@Override
	public void encode(String flowID, int[] s) {
		int r = rand.nextInt(Integer.MAX_VALUE);
		int ms = s.length;
		int j = r % ms;
		int i = GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]);
		boolean isMinus = Integer.numberOfLeadingZeros(i)>=1;
		j = (j % (m/2) + (m/2)) % (m/2);

		if (isMinus) j+=m/2;
		int hash_val = GeneralUtil.FNVHash1(String.valueOf(r));
		int leadingZeros = Integer.numberOfLeadingZeros(hash_val) + 1;
		leadingZeros = Math.min(leadingZeros, maxRegisterValue);
		if (getBitSetValue(HLLMatrix[j]) < leadingZeros) {
			setBitSetValue(j, leadingZeros);
		}
	}
	@Override
	public int getValue(String flowID, int[] s) {
		int ms = s.length;
		BitSet[] sketchMinus = new BitSet[ms];
		
		BitSet[] sketch = new BitSet[ms];
		//
		for (int j = 0; j < ms; j++) {
			//int i = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % (m/2) + (m/2)) % (m/2);
			//int i = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % (m) + (m)) % (m);
			int i = GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]);

			boolean isMinus = Integer.numberOfLeadingZeros(i)>=1;

			if (isMinus) { sketchMinus[j] = HLLMatrix[j];sketch[j] = HLLMatrix[j+(m/2)];}
			else{sketch[j] = HLLMatrix[j];sketchMinus[j] = HLLMatrix[j+m/2];}
		}
		/*
		if (getValue(sketch)>30000) {
			int sum =0;
				//System.out.println(getValue(sketchMinus));
			System.out.println("the length of ms is "+ms);
			System.out.println(getValue(sketch));
			for (int j = 0; j < ms; j++) {
				System.out.print(getBitSetValue(sketch[j])+" ");
				sum+=getBitSetValue(sketch[j]);
				
			}
			alpha=getAlpha(ms);
			System.out.println("teh average"+(1.0*sum/ms)+"    "+(alpha * ms*ms ));

		}
		*/
		return getValue(sketch)-getValue(sketchMinus);
	}

	
	public int getValue(long flowID, int[] s) {
		int ms = s.length;
		BitSet[] sketch = new BitSet[ms];

		for (int j = 0; j < ms; j++) {
			int i = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % m + m) % m;
			sketch[j] = HLLMatrix[i];
		}
		return getValue(sketch);
	}
	
	public int getValue(BitSet[] sketch) {
		Double result = 0.0;
		int zeros = 0;
		int len = sketch.length;
		for (int i = 0; i < len; i++) {
			//System.out.println(sketch[i]+"   "+i);
			if (getBitSetValue(sketch[i]) == 0) zeros++;
			result = result + Math.pow(2, -1.0 * getBitSetValue(sketch[i]));
			//result += 1.0*getBitSetValue(sketch[i]);
		}
		//result/=len;
		//result = len/0.78*Math.pow(2, result);
		//result = alpha*len*Math.pow(2, result);
		alpha = getAlpha(len);
		result = alpha * len * len / result;
		
		if (result <= 5.0 / 2.0 * len) {			// small flows
			result = 1.0 * len * Math.log(1.0 * len / Math.max(zeros, 1));
		} else if (result > Integer.MAX_VALUE / 30.0) {
			result = -1.0 * Integer.MAX_VALUE * Math.log(1 - result / Integer.MAX_VALUE);
		}
		return result.intValue();
	}
	
	public int getBitSetValue(BitSet b) {
		int res = 0;
		for (int i = 0; i < b.length(); i++) {
			if (b.get(i)) res += Math.pow(2, i);
		}
		return res;
	}
	
	public void setBitSetValue(int index, int value) {
		int i = 0;
		HLLMatrix[index].clear();
		while (value != 0 && i < HLLSize) {
			if ((value & 1) != 0) {
				HLLMatrix[index].set(i);
			} else {
				HLLMatrix[index].clear(i);
			}
			value = value >>> 1;
			i++;
		}
	}
	
	public double getAlpha(int m) {
		double a;
		if (m == 16) {
			a = 0.673;
		} else if (m == 32) {
			a = 0.697;
		} else if (m == 64) {
			a = 0.709;
		} else {
			a = 0.7213 / (1 + 1.079 / m);
		}
		return a;
	}
	@Override
	public void encodeSegment(long flowID, long elementID, int[] s, int w) {
		int ms = s.length;
		//int j = ((GeneralUtil.FNVHash1(flowID^GeneralvSkt.c1)^GeneralUtil.FNVHash1(elementID^GeneralvSkt.c2)) % ms + ms) % ms;
		//int j = ((GeneralUtil.FNVHash1(flowID,elementID)) % ms + ms) % ms;
		int j = ((GeneralUtil.FNVHash1(flowID^elementID)) % ms + ms) % ms;
		//int j = ((GeneralUtil.FNVHash1(elementID)) % ms + ms) % ms;
		//System.out.println(elementID^flowID);
		int temp = GeneralUtil.FNVHash1(flowID^s[j]);
		boolean isMinus = Integer.numberOfLeadingZeros(temp)>=1;
		int k = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % (w/2) + (w/2)) % (w/2);
		//int k = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % (w) + (w)) % w;

		int i = j * w + k;
		if (isMinus) i+=w/2;
		int hash_val = GeneralUtil.FNVHash1(elementID);
		int leadingZeros = Integer.numberOfLeadingZeros(hash_val) + 1;
		leadingZeros = Math.min(leadingZeros, maxRegisterValue);
		if (getBitSetValue(HLLMatrix[i]) < leadingZeros) {
			setBitSetValue(i, leadingZeros);
		}
		
	}
	@Override
	public void encodeSegment(String flowID, String elementID, int[] s, int w) {
		int ms = s.length;
		int j = ((GeneralUtil.FNVHash1(elementID)) % ms + ms) % ms;
		int k = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % w + w) % w;
		int i = j * w + k;
		int hash_val = GeneralUtil.FNVHash1(elementID);
		int leadingZeros = Integer.numberOfLeadingZeros(hash_val) + 1;
		leadingZeros = Math.min(leadingZeros, maxRegisterValue);
		if (getBitSetValue(HLLMatrix[i]) < leadingZeros) {
			setBitSetValue(i, leadingZeros);
			//System.out.println("from to " + getBitSetValue(HLLMatrix[i]));
			//System.out.println(leadingZeros+"-------------\t" + getBitSetValue(HLLMatrix[i]));
		}
		
	}

	@Override
	public void encodeSegment(int flowID, int elementID, int[] s, int w) {
		int ms = s.length;
		int j = (GeneralUtil.intHash(elementID) % ms + ms) % ms;
		int k = (GeneralUtil.intHash(flowID ^ s[j]) % w + w) % w;
		int i = j * w + k;
		int hash_val = GeneralUtil.intHash(elementID);
		int leadingZeros = Integer.numberOfLeadingZeros(hash_val) + 1;
		leadingZeros = Math.min(leadingZeros, maxRegisterValue);
		if (getBitSetValue(HLLMatrix[i]) < leadingZeros) {
			setBitSetValue(i, leadingZeros);
		}
	}

	@Override
	public void encodeSegment(String flowID, int[] s, int w) {
		int ms = s.length;
		int r = rand.nextInt(Integer.MAX_VALUE);
		int j = r % ms;
		int k = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % w + w) % w;
		int i = j * w + k;
		int hash_val = GeneralUtil.FNVHash1(String.valueOf(r));
		int leadingZeros = Integer.numberOfLeadingZeros(hash_val) + 1;
		leadingZeros = Math.min(leadingZeros, maxRegisterValue);
		if (getBitSetValue(HLLMatrix[i]) < leadingZeros) {
			setBitSetValue(i, leadingZeros);
		}
		
	}
	
	@Override
	public void encodeSegment(long flowID, int[] s, int w) {
		int ms = s.length;
		int r = rand.nextInt(Integer.MAX_VALUE);
		int j = r % ms;
		int k = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % w + w) % w;
		int i = j * w + k;
		int hash_val = GeneralUtil.FNVHash1(String.valueOf(r));
		int leadingZeros = Integer.numberOfLeadingZeros(hash_val) + 1;
		leadingZeros = Math.min(leadingZeros, maxRegisterValue);
		if (getBitSetValue(HLLMatrix[i]) < leadingZeros) {
			setBitSetValue(i, leadingZeros);
		}
	}

	@Override
	public void encodeSegment(int flowID, int[] s, int w) {
		int ms = s.length;
		int r = rand.nextInt(Integer.MAX_VALUE);
		int j = r % ms;
		int k = (GeneralUtil.intHash(flowID ^ s[j]) % w + w) % w;
		int i = j * w + k;
		int hash_val = GeneralUtil.intHash(r);
		int leadingZeros = Integer.numberOfLeadingZeros(hash_val) + 1;
		leadingZeros = Math.min(leadingZeros, maxRegisterValue);
		if (getBitSetValue(HLLMatrix[i]) < leadingZeros) {
			setBitSetValue(i, leadingZeros);
		}
	}
	
	@Override
	public int getValueSegment(String flowID, int[] s, int w) {
		int ms = s.length;
		BitSet[] sketch = new BitSet[ms];
		for (int j = 0; j < ms; j++) {
			int i = j * w + (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % w + w) % w;
			sketch[j] = HLLMatrix[i];
			
		}
		int result = getValue(sketch);
		return result;
	}
	public BitSet[] getMinSegment(String flowID, int[] s, int w, BitSet[] bb){
		int ms = s.length;
		BitSet[] sketch = new BitSet[ms];
		for (int j = 0; j < ms; j++) {
			int i = j * w + (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % w + w) % w;
			sketch[j] = HLLMatrix[i];
			if (getBitSetValue(sketch[j]) > getBitSetValue(bb[j]) ) {
				sketch[j] = bb[j];
			}
		}
		return sketch;
	}
	public BitSet[] getMaxSegment(String flowID, int[] s, int w, BitSet[] bb){
		int ms = s.length;
		BitSet[] sketch = new BitSet[ms];
		for (int j = 0; j < ms; j++) {
			int i = j * w + (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % w + w) % w;
			sketch[j] = HLLMatrix[i];
			if (getBitSetValue(sketch[j]) < getBitSetValue(bb[j]) ) {
				sketch[j] = bb[j];
			}
		}
		return sketch;
	}
	public BitSet[] getMinSegment(BitSet[] aa, BitSet[] bb){
		int ms = aa.length;
		BitSet[] sketch = new BitSet[ms];
		for (int j = 0; j < ms; j++) {
		
			if (getBitSetValue(aa[j]) > getBitSetValue(bb[j]) ) {
				sketch[j] = bb[j];
			}
			else
				sketch[j] = aa[j];
		}
		return sketch;
	}
	public BitSet[] getMaxSegment(BitSet[] aa, BitSet[] bb){
		int ms = aa.length;
		BitSet[] sketch = new BitSet[ms];
		for (int j = 0; j < ms; j++) {
		
			if (getBitSetValue(aa[j]) < getBitSetValue(bb[j]) ) {
				sketch[j] = bb[j];
			}
			else
				sketch[j] = aa[j];
		}
		return sketch;
	}
	@Override
	public int getValueSegment(long flowID, int[] s, int w) {
		int ms = s.length;
		BitSet[] sketch = new BitSet[ms];
		
		//System.out.println("-----flowid-----"+flowID);

		BitSet[] sketchMinus = new BitSet[ms];

		for (int j = 0; j < ms; j++) {
			//int i = j * w + (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % w + w) % w;
			int temp = GeneralUtil.FNVHash1(flowID^s[j]);
			boolean minus = Integer.numberOfLeadingZeros(temp)>=1;
			int i = j * w + (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % (w/2) + (w/2)) % (w/2);
			//int i = j * w + (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % (w) + (w)) % (w);

			//sketch[j] = HLLMatrix[i];			
			if (!minus) {sketch[j] = HLLMatrix[i];i+=w/2;sketchMinus[j]=HLLMatrix[i];}
			else {sketchMinus[j] = HLLMatrix[i];i+=w/2;sketch[j]=HLLMatrix[i];}	
		}
		int result = getValue(sketch);
		int resultMinus =getValue(sketchMinus);
		//if (result-resultMinus>=5000) {
			//if(rand.nextDouble()<0.01) 
				//System.out.println("-----"+flowID+"-----"+(result)+"----"+resultMinus);			

			//for (int j = 0; j < ms; j++) {
				//System.out.println("-----"+j+"-----"+getBitSetValue(sketch[j])+"-----"+getBitSetValue(sketchMinus[j]));			
			//}
		//}
		double ratio=1.0;
		return (result-(int)(ratio*resultMinus))>=1?(result-(int)(ratio*resultMinus)):1;
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
			sketch_sampled[j++] = HLLMatrix[k];
		}
		//System.out.println("Sampled virtual sketch size: " + index_sampled.size() + "\nOriginal virtual sketch size: " + ms);
		return getValue(sketch_sampled);
	}
	
	@Override
	public GeneralDataStructure join(GeneralDataStructure gds) {
		HyperLogLog hll = (HyperLogLog) gds;
		for (int i = 0; i < m; i++) {
			if (getBitSetValue(HLLMatrix[i]) < getBitSetValue(hll.HLLMatrix[i]))
				HLLMatrix[i] = hll.HLLMatrix[i];
		}
		return this;
	}
	@Override
	public GeneralDataStructure join(GeneralDataStructure gds,int w,int i) {
		HyperLogLog hll = (HyperLogLog) gds;		
		for(int j=0;j<w;j++) {
			if (getBitSetValue(HLLMatrix[i]) < getBitSetValue(hll.HLLMatrix[i*w+j]))
				HLLMatrix[i] = hll.HLLMatrix[i*w+j];
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
}

