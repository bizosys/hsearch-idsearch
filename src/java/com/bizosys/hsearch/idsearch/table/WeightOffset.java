package com.bizosys.hsearch.idsearch.table;

public class WeightOffset 
{
	public static byte[] toByteWeightAndOffset(int weight, int offset) 
	{
		return new byte[]
				{
					(byte)(weight >> 56), 
					(byte)(weight >> 48 ), 
					(byte)(weight >> 40 ), 
					(byte)(weight >> 32 ), 
					(byte)(offset >> 24 ), 
					(byte)(offset >> 16 ), 
					(byte)(offset >> 8 ), 
					(byte)(offset ) 
				};		
	}
	
	public static int[] getWeightAndOffset(int index, final byte[] inputBytes) 
	{
		if ( 0 == inputBytes.length) return new int [] {0,0};
		
		int weight = (int)    ( 
			( (inputBytes[index] & 0xffL ) << 24 ) + 
			( (inputBytes[++index] & 0xff ) << 16 ) + 
			( (inputBytes[++index] & 0xff ) << 8 ) + 
			( inputBytes[++index] & 0xff )              );

		int offset = (int)    ( 
		( (inputBytes[++index] & 0xffL ) << 24 ) + 
		( (inputBytes[++index] & 0xff ) << 16 ) + 
		( (inputBytes[++index] & 0xff ) << 8 ) + 
		( inputBytes[++index] & 0xff )              );
		
		return new int[]{weight, offset};
	}	
	
	private static long getLong(int index, final byte[] inputBytes) 
	{
		if ( 0 == inputBytes.length) return 0;
		
		long longVal = ( ( (long) (inputBytes[index]) )  << 56 )  + 
		( (inputBytes[++index] & 0xffL ) << 48 ) + 
		( (inputBytes[++index] & 0xffL ) << 40 ) + 
		( (inputBytes[++index] & 0xffL ) << 32 ) + 
		( (inputBytes[++index] & 0xffL ) << 24 ) + 
		( (inputBytes[++index] & 0xff ) << 16 ) + 
		( (inputBytes[++index] & 0xff ) << 8 ) + 
		( inputBytes[++index] & 0xff );
		return longVal;
	}
	
	
	
	public static void main(String[] args) 
	{
		int[] weights = new int[] {Integer.MAX_VALUE, Integer.MAX_VALUE -1, 100000, 10000, 1000, 100, 1, 0};
		int[] offsets =  new int[] {0, 1, 100, 1000, 10000, 100000,Integer.MAX_VALUE - 1,  Integer.MAX_VALUE};

		for ( int i=0; i< weights.length; i++) 
		{
			int weight = weights[i];
			int offset = offsets[i];
			
			System.out.println("--------------------------------");
			System.out.print(getLong(0, toByteWeightAndOffset(weight,offset) ));
			System.out.print( "  == " + weight + "|" + offset );
			System.out.print(  "  (  " + getWeightAndOffset(0, toByteWeightAndOffset(weight,offset) ) [0]);
			System.out.println(  "  :  " + getWeightAndOffset(0, toByteWeightAndOffset(weight,offset) ) [1] + "  )");
		}
	}
}