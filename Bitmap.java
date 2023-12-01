package bsktHLLbased;

import java.util.*;

public class Bitmap extends GeneralDataStructure{
	/** parameters for bitmap */
	public int bitmapValue;										// the value derived from the bitmap
	public static String name = "Bitmap";						// bitmap data structure name
	public int arrayLength;								// the size of the bit array
	
	public BitSet B;
	
	public Bitmap(int s) {
		super();
		arrayLength = s;
		B = new BitSet(s);
	}

	@Override
	public String getDataStructureName() {
		return name;
	}

	@Override
	public int getUnitSize() {
		return arrayLength;
	}
	
	@Override
	public BitSet getBitmaps() {
		return B;
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
		return null;
	};	

	@Override
	public int getValue() {
		if (arrayLength == 1) {
			return B.cardinality();
		}
		return getValue(B);
	}
	
	@Override
	public void encode() {
		int k = rand.nextInt(arrayLength);
		B.set(k);
	}
	
	@Override
	public void encode(String elementID) {
		int k = (GeneralUtil.FNVHash1(elementID) % arrayLength + arrayLength) % arrayLength;
		B.set(k);
	}
	
	@Override
	public void encode(int elementID) {
		int k = (GeneralUtil.intHash(elementID) % arrayLength + arrayLength) % arrayLength;
		B.set(k);
	}
	
	@Override


	public int encodeBF(String flowID, String elementID,int[] s,String label) {
		//System.out.println(s.length);
		int m = s.length;
		int zerosNum =0;
		for (int j=0;j<m;j++) {
				int i = (GeneralUtil.intHash(GeneralUtil.FNVHash1(elementID+flowID) ^ s[j]) % arrayLength + arrayLength) % arrayLength;
				if (!B.get(i))zerosNum++;
				if (label=="after") B.set(i);
		}
		return zerosNum;
	}
	@Override
	public void encodeSegment(String flowID, int[] s, int w) {
		int ms = s.length;
		int j = rand.nextInt(ms);
		int k = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % w + w) % w;
		int i = j * w + k;
		B.set(i);
	}
	
	@Override
	public void encodeSegment(long flowID, int[] s, int w) {
		int ms = s.length;
		int j = rand.nextInt(ms);
		int k = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % w + w) % w;
		int i = j * w + k;
		B.set(i);
	}
	
	@Override
	public void encodeSegment(int flowID, int[] s, int w) {
		int ms = s.length;
		int j = rand.nextInt(ms);				// (GeneralUtil.intHash(flowID) % ms + ms) % ms;		//
		int k = (GeneralUtil.intHash(flowID ^ s[j]) % w + w) % w;
		int i = j * w + k;
		B.set(i);
	}
	
	@Override
	public void encode(int flowID, int[] s) {
		int m = s.length;
		int j = rand.nextInt(m);
		int i = (GeneralUtil.intHash(flowID ^ s[j]) % arrayLength + arrayLength) % arrayLength;
		B.set(i);
	}

	@Override
	public void encode(String flowID, String elementID, int[] s) {
		int m = s.length;
		int j = (GeneralUtil.FNVHash1(elementID)%m+m)%m;
		int i = GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) ;
		boolean isMinus = Integer.numberOfLeadingZeros(i)>=1;
		j = (j%(arrayLength/2)+(arrayLength/2))%(arrayLength/2);
		if (isMinus) j+= arrayLength/2;
		B.set(j);
	}
	@Override
	public void encode(long flowID, long elementID, int[] s) {
		//int m = s.length;
		//int j = (GeneralUtil.FNVHash1(elementID) % m + m) % m;
		int i = (GeneralUtil.intHash(GeneralUtil.FNVHash1(elementID) ^ s[0]) % arrayLength + arrayLength) % arrayLength;
		B.set(i);
	}
	
	@Override
	public void encode(int flowID, int elementID, int[] s) {
		int m = s.length;
		int j = (GeneralUtil.intHash(elementID) % m + m) % m;
		int i = (GeneralUtil.intHash(flowID ^ s[j]) % arrayLength + arrayLength) % arrayLength;
		B.set(i);
	}
	@Override
	public GeneralDataStructure[] getsplit(int m,int w) {
		GeneralDataStructure[]  B=new Bitmap[m];
		for(int i=0;i<m;i++) {
			B[i]=new Bitmap(w);
			for(int j=0;j<w/m;j++)
				B[i].getBitmaps().set(j, this.getBitmaps().get(i*(w/m)+j));
		}
		return B;
	}
	@Override
	public void encodeSegment(long flowID, long elementID, int[] s, int w) {
		int m = s.length;
		int j = (GeneralUtil.FNVHash1(elementID^flowID) % m + m) % m;
		int temp = GeneralUtil.FNVHash1(flowID^s[j]);
		boolean isMinus = Integer.numberOfLeadingZeros(temp)>=1;
		int k = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % (w/2) + (w/2)) % (w/2);
		int i = j * w + k;
		if (isMinus) i+=w/2;
		B.set(i);
	}
	@Override
	public void encodeSegment(String flowID, String elementID, int[] s, int w) {
		int m = s.length;
		int j = (GeneralUtil.FNVHash1(elementID) % m + m) % m;
		int k = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % w + w) % w;
		int i = j * w + k;
		B.set(i);
	}
	
	@Override
	public void encodeSegment(int flowID, int elementID, int[] s, int w) {
		int m = s.length;
		int j = (GeneralUtil.intHash(elementID) % m + m) % m;
		int k = (GeneralUtil.intHash(flowID ^ s[j]) % w + w) % w;
		int i = j * w + k;
		B.set(i);
	}
	public void encode(String flowID, int[] s) {
		int m = s.length;
		int j = rand.nextInt(m);
		int i = GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) ;
		boolean isMinus = Integer.numberOfLeadingZeros(i)>=1;
		j = (j%(arrayLength/2)+(arrayLength/2))%(arrayLength/2);
		if (isMinus) j+= arrayLength/2;
		B.set(j);
		
	}
	@Override
	public int getValue(String flowID, int[] s) {
		int ms = s.length;
		BitSet b = new BitSet(ms);
		BitSet bMinus = new BitSet(ms);

		for (int j = 0; j < ms; j++) {
			int i = GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]);
			boolean isMinus = Integer.numberOfLeadingZeros(i)>=1;
			if (isMinus) {if (B.get(j)) bMinus.set(j);if (B.get(j+ms)) b.set(j);}
			else {if (B.get(j)) b.set(j);if (B.get(j+ms)) bMinus.set(j);}

		}
		return getValue(b)-getValue(bMinus);
	}
	
	@Override
	public int getValue(long flowID, int[] s) {
		int ms = s.length;
		BitSet b = new BitSet(ms);
		for (int j = 0; j < ms; j++) {
			int i = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % arrayLength + arrayLength) % arrayLength;
			if (B.get(i)) b.set(j);
		}
		return getValue(b);
	}
	
	@Override
	public int getValueSegment(String flowID, int[] s, int w) {
		int ms = s.length;
		BitSet b = new BitSet(ms);
		for (int j = 0; j < ms; j++) {
			int i = j * w + (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % w + w) % w;
			if (B.get(i)) b.set(j);
		}
		return getValue(b);
	}
	
	@Override
	public int getValueSegment(long flowID, int[] s, int w) {
		int ms = s.length;
		BitSet b = new BitSet(ms);
		BitSet sketchMinus = new BitSet(ms);

		for (int j = 0; j < ms; j++) {
			int temp = GeneralUtil.FNVHash1(flowID^s[j]);
			boolean minus = Integer.numberOfLeadingZeros(temp)>=1;
			int i = j * w + (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % (w/2) + (w/2)) % (w/2);
			if (!minus) {if (B.get(i)) b.set(j);i+=w/2;if (B.get(i)) sketchMinus.set(j);}
			else {if (B.get(i)) sketchMinus.set(j);i+=w/2;if (B.get(i)) b.set(j);}	
			if (B.get(i)) b.set(j);
		}
		return getValue(b)-getValue(sketchMinus);
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
		BitSet sketch_sampled = new BitSet(mss);
		HashSet<Integer> index_sampled = new HashSet<>();
		while (index_sampled.size() < mss) {
			if (rand.nextDouble() < 1.0 / sample_ratio) {
				index_sampled.add(indexes[i]);
				// System.out.println("Sampled!");
			}
			i = (i + 1) % ms;
		}
		int j = 0;
		for (int k: index_sampled) {
			if (B.get(k)) sketch_sampled.set(j++);
		}
		// System.out.println("Sampled virtual sketch size: " + index_sampled.size() + "\nOriginal virtual sketch size: " + ms);
		return getValue(sketch_sampled);
	}
	
	public int getValue(BitSet b) {
		int zeros = 0;
		int len = b.size();
		for (int i = 0; i < len; i++) {
			if (!b.get(i)) zeros++;
		}
		Double result = -len * Math.log(1.0 * Math.max(zeros, 1)/ len);
		return result.intValue();
	}
	@Override
	public GeneralDataStructure join(GeneralDataStructure gds,int w,int i) {
		Bitmap b = (Bitmap) gds;
		for(int j=0;j<w;j++)
			if((b.getBitmaps().get(i*w+j))){
				B.set(i);
				break;
			}
		return this;
		
	}
	@Override
	public GeneralDataStructure join(GeneralDataStructure gds) {
		Bitmap b = (Bitmap) gds;
		B.or(b.B);
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
	protected int getValue(BitSet[] temp) {
		// TODO Auto-generated method stub
		return 0;
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
