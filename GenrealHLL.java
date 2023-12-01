package bsktHLLbased;import java.util.BitSet;
import java.util.HashSet;
import java.util.Random;

public class GenrealHLL {
	public String name = "GeneralHLL";
	public int m;
	public int size;
	public int l;
	public double p;
	public int max_value;
	public int control;
	public int[] HLL;
	public Random random;
	public long[] seeds;
	public double base;
	public int nRandom;
	GenrealHLL(int m, int size, double base) {
	   this.m = m;
	   this.size = size;
	   l = size * m;
	   p = 1.0;
	   this.base  = base;
	   max_value = (1 << size) - 1;
	   HLL = new int[m];
	   random = new Random();
	 
	   HashSet<Long> set = new HashSet<>();
	   seeds = new long[m + 1];
	   for (int i = 0; i < seeds.length; i++) {
		   seeds[i] = random.nextLong();
		   while (set.contains(seeds[i])) {
			   seeds[i] = random.nextLong();
		   }
		   set.add(seeds[i]);
	   }
	}
	
	public static int getMaxNumber(int M, int size) {
		return M / size;
	}
	
	public static void setBitSetValue(BitSet set, int start, int length, int value) {
		for (int i = start; i < start + length; i++) {
			if ((value & 1) == 0) {
				set.clear(i);
			} else {
				set.set(i);
			}
			value >>= 1;
		}
	}
	
	public static int getBitSetValue(BitSet set, int start, int length) {
		int value = 0;
		for (int i = start; i < start + length; i++) {
			if (set.get(i)) {
				value |= 1 << (i - start);
			}
		}
		return value;
	}
	
	public static long hash64shift(long key)
	{
	  key = (~key) + (key << 21); // key = (key << 21) - key - 1;
	  key = key ^ (key >>> 24);
	  key = (key + (key << 3)) + (key << 8); // key * 265
	  key = key ^ (key >>> 14);
	  key = (key + (key << 2)) + (key << 4); // key * 21
	  key = key ^ (key >>> 28);
	  key = key + (key << 31);
	  return key;
	}
	public int GeometricHash(long element) {
		
		
			int hashV = (int)((hash64shift(element ^ seeds[0])%Integer.MAX_VALUE+Integer.MAX_VALUE)%Integer.MAX_VALUE);
			double hashV1 = hashV *1.0 / Integer.MAX_VALUE;
			int v = (int) Math.ceil(Math.log(1-hashV1)/Math.log(base));
			return v;
		
	}
	public double encode(long element) {
		int leadingZeros = GeometricHash(element);
		leadingZeros = Math.min(leadingZeros, max_value);
		int i = (int)(hash64shift(element ^ seeds[1]) % (long)m);
		i = (i + m) % m;
		int original = HLL[i];
		double result =  p;
		if (leadingZeros <= original) {
			return -1.0;
		} else {
			p -= Math.pow(base, original) / (double)m;
			HLL[i] =  leadingZeros;
			if (leadingZeros < max_value) {
				p += Math.pow(base, leadingZeros) / (double)m;
			}	
		}
		return result;
	}
}