package com.bizosys.hsearch.kv.indexer;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import com.bizosys.hsearch.util.LineReaderUtil;

public class KVIndexerPartioner extends Partitioner<Text, BytesWritable> {

	@Override
	public int getPartition(Text key, BytesWritable value, int numReduceTasks) {

		String structureKeyStr = key.toString();
		String[] keyParts = new String[4];
		LineReaderUtil.fastSplit(keyParts, structureKeyStr, '\t');
		String partionKey = keyParts[0] + "\t" + keyParts[1];
		int reducer = (partionKey.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
		return reducer;
		
	}
}
