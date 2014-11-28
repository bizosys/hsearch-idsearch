package com.bizosys.hsearch.kv.indexer;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;

public class HsearchCounterUtil {
	
	public static enum HsearchCounters {
		  Total_Input_Records,
		  Total_Index_Records,
		  Total_Input_Bytes,
		  Total_Index_Bytes,
		  Map_Spill_Bucket_Size,
		  Map_Spill_Bucket_Count,
		  Max_Map_Buffer_Size,
		  Index_Map_Time,
		  Index_Reduce_Time
	};
	
	public static final String processCounters(final Job job , final String tableName , final String outputFolder) throws IOException {

		CounterGroup dataswftCounters = job.getCounters().getGroup(HsearchCounters.class.getName());
		
		StringBuilder sb = new StringBuilder(256);
		for (Counter counter : dataswftCounters) {
			sb.append(counter.getName()).append("=").append(counter.getValue());
			sb.append("\n");
		}
		String counterData = sb.toString();
		return counterData;
	}
}