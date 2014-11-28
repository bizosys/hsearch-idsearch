package com.bizosys.hsearch.kv.indexer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.bizosys.hsearch.byteutils.SortedBytesArray;
import com.bizosys.hsearch.byteutils.SortedBytesBoolean;
import com.bizosys.hsearch.byteutils.SortedBytesChar;
import com.bizosys.hsearch.byteutils.SortedBytesDouble;
import com.bizosys.hsearch.byteutils.SortedBytesFloat;
import com.bizosys.hsearch.byteutils.SortedBytesInteger;
import com.bizosys.hsearch.byteutils.SortedBytesLong;
import com.bizosys.hsearch.byteutils.SortedBytesShort;
import com.bizosys.hsearch.byteutils.SortedBytesString;
import com.bizosys.hsearch.kv.impl.Datatype;

public final class KVSpillerBucket {
	
	static final SortedBytesDouble doubleSerializer = SortedBytesDouble.getInstanceDouble();
	static final SortedBytesString stringSerializer = SortedBytesString.getInstanceString();
	static final SortedBytesFloat floatSerializer = SortedBytesFloat.getInstanceFloat();
	static final SortedBytesShort shortSerializer = SortedBytesShort.getInstanceShort();
	static final SortedBytesInteger integerSerializer = SortedBytesInteger.getInstanceInt();
	static final SortedBytesBoolean booleanSerializer = SortedBytesBoolean.getInstanceBoolean();
	static final SortedBytesChar byteSerializer = SortedBytesChar.getInstanceChar();
	static final SortedBytesLong longSerializer = SortedBytesLong.getInstanceLong();
	
	static final boolean FLOAT_ACCURACY = false;
	
	byte datatype = 0;
	int size = 0;
	int currentFillCounter = 0;
	int dataSize = 0;
	final int MAX_FILL_SIZE = 4096;
	byte[] keyBytesReusable = new byte[4 * MAX_FILL_SIZE];
	
	int[] currentKeyFillStorage = new int[MAX_FILL_SIZE];
	final List<int[]> currentKeyFillStorageL = new ArrayList<int[]>(1024);
	
	boolean[] currentValFillStorageBoolean = null;
	byte[] currentValFillStorageByte = null;
	short[] currentValFillStorageShort = null;
	int[] currentValFillStorageInt = null;
	long[] currentValFillStorageLong = null;
	float[] currentValFillStorageFloat = null;
	double[] currentValFillStorageDouble = null;
	String[] currentValFillStorageString = null;

	
	List<boolean[]> currentValFillStorageBooleanL = null;
	List<byte[]> currentValFillStorageByteL = null;
	List<short[]> currentValFillStorageShortL = null;
	List<int[]> currentValFillStorageIntL = null;
	List<long[]> currentValFillStorageLongL = null;
	List<float[]> currentValFillStorageFloatL = null;
	List<double[]> currentValFillStorageDoubleL = null;
	List<String[]> currentValFillStorageStringL = null;
	
	
	public KVSpillerBucket(final byte datatype) {
		
		this.datatype = datatype;
		switch (this.datatype) {
			case Datatype.BOOLEAN:
				currentValFillStorageBoolean = new boolean[MAX_FILL_SIZE];
				currentValFillStorageBooleanL = new ArrayList<boolean[]>(1024);
				break;
				
			case Datatype.BYTE:
				currentValFillStorageByte = new byte[MAX_FILL_SIZE];
				currentValFillStorageByteL = new ArrayList<byte[]>(1024);
				break;
			
			case Datatype.SHORT:
				currentValFillStorageShort = new short[MAX_FILL_SIZE];
				currentValFillStorageShortL = new ArrayList<short[]>(1024);
				break;
			
			case Datatype.INTEGER:
				currentValFillStorageInt = new int[MAX_FILL_SIZE];
				currentValFillStorageIntL = new ArrayList<int[]>(1024);
				break;
			
			case Datatype.FLOAT:
				currentValFillStorageFloat = new float[MAX_FILL_SIZE];
				currentValFillStorageFloatL = new ArrayList<float[]>(1024);
				break;
			
			case Datatype.LONG:
				currentValFillStorageLong = new long[MAX_FILL_SIZE];
				currentValFillStorageLongL = new ArrayList<long[]>(1024);
				break;
			
			case Datatype.DOUBLE:
				currentValFillStorageDouble = new double[MAX_FILL_SIZE];
				currentValFillStorageDoubleL = new ArrayList<double[]>(1024);
				break;
			
			case Datatype.STRING:
				currentValFillStorageString = new String[MAX_FILL_SIZE];
				 currentValFillStorageStringL = new ArrayList<String[]>(1024);
				break;
			default:
		}
	}
	
	public final int add(final int k , final String v){

		boolean flush = false; 
		currentKeyFillStorage[currentFillCounter] = k;
		if ( currentFillCounter >= MAX_FILL_SIZE - 1) {
			flush = true; 
			currentKeyFillStorageL.add(currentKeyFillStorage);
			currentKeyFillStorage = new int[MAX_FILL_SIZE];
		}
		size++;
		
		switch (this.datatype) {
			case Datatype.BOOLEAN:
				currentValFillStorageBoolean[currentFillCounter] = Boolean.parseBoolean(v);
				currentFillCounter++;
				if ( flush ) {
					currentValFillStorageBooleanL.add(currentValFillStorageBoolean);
					currentFillCounter = 0; currentValFillStorageBoolean = new boolean[MAX_FILL_SIZE];
				}
				return 5;
			case Datatype.BYTE:
				currentValFillStorageByte[currentFillCounter] = Byte.parseByte(v);
				currentFillCounter++;
				if ( flush ) {
					currentValFillStorageByteL.add(currentValFillStorageByte);
					currentFillCounter = 0; currentValFillStorageByte = new byte[MAX_FILL_SIZE];
				}
				return  5;
			case Datatype.SHORT:
				currentValFillStorageShort[currentFillCounter] = Short.parseShort(v);
				currentFillCounter++;
				if ( flush ) {
					currentValFillStorageShortL.add(currentValFillStorageShort);
					currentFillCounter = 0; currentValFillStorageShort = new short[MAX_FILL_SIZE];
				}
				return 6;
			case Datatype.INTEGER:
				currentValFillStorageInt[currentFillCounter] = Integer.parseInt(v);
				currentFillCounter++;
				if ( flush ) {
					currentValFillStorageIntL.add(currentValFillStorageInt);
					currentFillCounter = 0; currentValFillStorageInt = new int[MAX_FILL_SIZE];
				}
				return 8;
			case Datatype.FLOAT:
				if ( FLOAT_ACCURACY ) 
					currentValFillStorageFloat[currentFillCounter] = Float.parseFloat(v);
				else 
					currentValFillStorageFloat[currentFillCounter] = parseFloat(v);
					
				currentFillCounter++;
				if ( flush ) {
					currentValFillStorageFloatL.add(currentValFillStorageFloat);
					currentFillCounter = 0; currentValFillStorageFloat = new float[MAX_FILL_SIZE];
				}
				return 8;
			case Datatype.LONG:
				currentValFillStorageLong[currentFillCounter] = Long.parseLong(v);
				currentFillCounter++;
				if ( flush ) {
					currentValFillStorageLongL.add(currentValFillStorageLong);
					currentFillCounter = 0; currentValFillStorageLong = new long[MAX_FILL_SIZE];
				}
				return 12;
			case Datatype.DOUBLE:
				currentValFillStorageDouble[currentFillCounter] = Double.parseDouble(v);
				currentFillCounter++;
				if ( flush ) {
					currentValFillStorageDoubleL.add(currentValFillStorageDouble);
					currentFillCounter = 0; currentValFillStorageDouble = new double[MAX_FILL_SIZE];
				}
				return 12;
			case Datatype.STRING:
				currentValFillStorageString[currentFillCounter] = v;
				currentFillCounter++;
				if ( flush ) {
					currentValFillStorageStringL.add(currentValFillStorageString);
					currentFillCounter = 0; currentValFillStorageString = new String[MAX_FILL_SIZE];
				}
				return v.length() + 12;
			default:
				System.err.println("Warning: Unknown Datatype : " + this.datatype) ;
				size--;
				return 0;
		}
		
	}
	
	public final int size(){
		return size;
	}

	public final byte[] toBytes() throws IOException{

		if(currentFillCounter > 0) 
			currentKeyFillStorageL.add(currentKeyFillStorage);
		
		byte[] kB = null;
		
		if (size == MAX_FILL_SIZE) {
			kB = integerSerializer.toBytes(currentKeyFillStorageL,size,keyBytesReusable);
		} else {
			kB = integerSerializer.toBytes(currentKeyFillStorageL,size);
		}
		currentKeyFillStorageL.clear();

		byte[] vB = null;

		switch (this.datatype) {
	
			case Datatype.BOOLEAN:
				if(currentFillCounter > 0) 
					currentValFillStorageBooleanL.add(currentValFillStorageBoolean);
				currentFillCounter = 0;
				vB = booleanSerializer.toBytes(currentValFillStorageBooleanL,size);
				break;
			case Datatype.BYTE:
				if(currentFillCounter > 0) 
					currentValFillStorageByteL.add(currentValFillStorageByte);
				currentFillCounter  = 0;
				vB = byteSerializer.toBytes(currentValFillStorageByteL,size);
				break;
			case Datatype.SHORT:
				if(currentFillCounter > 0) 
					currentValFillStorageShortL.add(currentValFillStorageShort);
				vB = shortSerializer.toBytes(currentValFillStorageShortL,size);
				currentFillCounter  = 0;
				break;
			case Datatype.INTEGER:
				if(currentFillCounter > 0) 
					currentValFillStorageIntL.add(currentValFillStorageInt);
				vB = integerSerializer.toBytes(currentValFillStorageIntL,size);
				currentFillCounter  = 0;
				break;
			case Datatype.FLOAT:
				if(currentFillCounter > 0) 
					currentValFillStorageFloatL.add(currentValFillStorageFloat);
				vB = floatSerializer.toBytes(currentValFillStorageFloatL,size);
				currentFillCounter  = 0;
				break;
			case Datatype.LONG:
				if(currentFillCounter > 0) 
					currentValFillStorageLongL.add(currentValFillStorageLong);
				vB = longSerializer.toBytes(currentValFillStorageLongL,size);
				currentFillCounter  = 0;
				break;
			case Datatype.DOUBLE:
				if(currentFillCounter > 0) 
					currentValFillStorageDoubleL.add(currentValFillStorageDouble);
				vB = doubleSerializer.toBytes(currentValFillStorageDoubleL,size);
				currentFillCounter  = 0;
				break;
			case Datatype.STRING:
				if(currentFillCounter > 0) 
					currentValFillStorageStringL.add(currentValFillStorageString);
				vB = stringSerializer.toBytes(currentValFillStorageStringL,size);
				currentFillCounter  = 0;
				break;
			default:
				throw new IOException("Unknwn data type : " + this.datatype);
		}
		
		
		this.clear();
		return SortedBytesArray.getInstanceArr().toBytes(kB, vB);

	}

	public final void clear(){
		
		currentKeyFillStorageL.clear();
		currentKeyFillStorage = new int[MAX_FILL_SIZE];
		size = 0;
		
		switch (this.datatype) {
			case Datatype.BOOLEAN:
				currentValFillStorageBooleanL.clear();
				currentValFillStorageBoolean = new boolean[MAX_FILL_SIZE];
				break;
			
			case Datatype.BYTE:
				currentValFillStorageByteL.clear();
				currentValFillStorageByte = new byte[MAX_FILL_SIZE];
				break;

			case Datatype.SHORT:
				currentValFillStorageShortL.clear();
				currentValFillStorageShort = new short[MAX_FILL_SIZE];
				break;
			
			case Datatype.INTEGER:
				currentValFillStorageIntL.clear();
				currentValFillStorageInt = new int[MAX_FILL_SIZE];
				break;

			case Datatype.FLOAT:
				currentValFillStorageFloatL.clear();
				currentValFillStorageFloat = new float[MAX_FILL_SIZE];
				break;

			case Datatype.LONG:
				currentValFillStorageLongL.clear();
				currentValFillStorageLong = new long[MAX_FILL_SIZE];
				break;

			case Datatype.DOUBLE:
				currentValFillStorageDoubleL.clear();
				currentValFillStorageDouble = new double[MAX_FILL_SIZE];
				break;

			case Datatype.STRING:
				currentValFillStorageStringL.clear();
				currentValFillStorageString = new String[MAX_FILL_SIZE];
				break;
		}
	}
	
	
	public static float parseFloat(String f) {
		final int len   = f.length();
		float     ret   = 0f;         // return value
		int       pos   = 0;          // read pointer position
		int       part  = 0;          // the current part (int, float and sci parts of the number)
		boolean   neg   = false;      // true if part is a negative number
	 
		// find start
		while (pos < len && (f.charAt(pos) < '0' || f.charAt(pos) > '9') && f.charAt(pos) != '-' && f.charAt(pos) != '.')
			pos++;
	 
		// sign
		if (f.charAt(pos) == '-') { 
			neg = true; 
			pos++; 
		}
	 
		// integer part
		while (pos < len && !(f.charAt(pos) > '9' || f.charAt(pos) < '0'))
			part = part*10 + (f.charAt(pos++) - '0');
		ret = neg ? (float)(part*-1) : (float)part;
	 
		// float part
		if (pos < len && f.charAt(pos) == '.') {
			pos++;
			int mul = 1;
			part = 0;
			while (pos < len && !(f.charAt(pos) > '9' || f.charAt(pos) < '0')) {
				part = part*10 + (f.charAt(pos) - '0'); 
				mul*=10; pos++;
			}
			ret = neg ? ret - (float)part / (float)mul : ret + (float)part / (float)mul;
		}
	 
		// scientific part
		if (pos < len && (f.charAt(pos) == 'e' || f.charAt(pos) == 'E')) {
			pos++;
			neg = (f.charAt(pos) == '-'); pos++;
			part = 0;
			while (pos < len && !(f.charAt(pos) > '9' || f.charAt(pos) < '0')) {
				part = part*10 + (f.charAt(pos++) - '0'); 
			}
			if (neg)
				ret = ret / (float)Math.pow(10, part);
			else
				ret = ret * (float)Math.pow(10, part);
		}	
		return ret;
	}
}
