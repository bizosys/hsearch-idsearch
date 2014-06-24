/*
* Copyright 2013 Bizosys Technologies Limited
*
* Licensed to the Bizosys Technologies Limited (Bizosys) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The Bizosys licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.bizosys.hsearch.kv.indexing;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.Term;

import com.bizosys.hsearch.byteutils.Storable;
import com.bizosys.hsearch.hbase.HReader;
import com.bizosys.hsearch.hbase.HWriter;
import com.bizosys.hsearch.hbase.NV;
import com.bizosys.hsearch.hbase.RecordScalar;
import com.bizosys.hsearch.idsearch.util.IdSearchLog;
import com.bizosys.hsearch.kv.impl.FieldMapping;
import com.bizosys.hsearch.kv.impl.FieldMapping.Field;
import com.bizosys.hsearch.kv.indexing.KVIndexer.KV;
import com.bizosys.hsearch.util.HSearchConfig;
import com.bizosys.hsearch.util.HSearchLog;
import com.bizosys.hsearch.util.Hashing;
import com.bizosys.hsearch.util.LineReaderUtil;
import com.bizosys.unstructured.AnalyzerFactory;

public class KVMapperBase {

	private static final String EMPTY = "";
	private static final char PLUS = '+';
	private static final byte[] LONG_0 = Storable.putLong(0);
	/**
	 * Field Positions defined in schema
	 */
	protected Set<Integer> neededPositions = null;  
	protected FieldMapping fm = null;

	/**
	 * Concatination of multiple fields to form a partition key.
	 */
	protected Map<Integer,String> rowIdMap = new HashMap<Integer, String>();
	
	protected StringBuilder appender = new StringBuilder();
	protected String tableName = null;
	protected NV kv = null;

	com.bizosys.hsearch.util.conf.Configuration hConfig = HSearchConfig.getInstance().getConfiguration(); 
	private final int mergeKeyCacheSize = hConfig.getInt("internalid.increment.chunksize", 1000);
	
	protected long incrementalIdSeekPosition = 0;

	/**
	 * For each Ids, the Counter. This could grow huge.
	 */
	private final int expectedIndexRows = hConfig.getInt("expected.index.rows.count", 1000000);
	protected Map<String, Counter> ids = new HashMap<String, Counter>(expectedIndexRows,0.80F);

	/**
	 * For each Ids, the Counter. This could grow huge.
	 */
	private final long aMapTime = hConfig.getLong("report.amaptime.cutoff", 45000L);
	private final boolean isReportMapTime = ( aMapTime > 0); 
	
	String fldValue = null;
	boolean isFieldNull = false;
	String rowKeyP1 = EMPTY;
	String rowKeyP2 = EMPTY;
	String rowVal = EMPTY;
	String mergeId = EMPTY;
	
	String partName = null;

	long startTime = -1L;
	long endTime = -1L;
	
	boolean isFirstTime = true;
	LineReaderUtil slowRecordBuilder = new LineReaderUtil(); 
	
	IOException setupExp = null;
		
	private static class Counter{
		public long seekPointer = -1;
		public long maxPointer = Long.MAX_VALUE;

		public Counter(long seekPointer, long maxPointer){
			this.seekPointer = seekPointer;
			this.maxPointer = maxPointer;
		}
	}

	public KV onMap(KV kv ) {
		return kv;
	}

	@SuppressWarnings("rawtypes")
	public void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		StringBuilder sb = new StringBuilder();

		try {
			String path = conf.get(KVIndexer.XML_FILE_PATH);
			fm = KVIndexer.createFieldMapping(conf, path, sb);
			AnalyzerFactory.getInstance().init(fm);
			neededPositions = fm.sourceSeqWithField.keySet();
			KVIndexer.FIELD_SEPARATOR = fm.fieldSeparator;

			kv = new NV(fm.familyName.getBytes(), "1".getBytes());
			this.tableName = fm.tableName;

		} catch (Exception ex) {
			setupExp = new IOException("UNable to instantiate the Analyzer Factory. Check classpath");
		}

	}
	
	@SuppressWarnings({ "rawtypes" })
	protected void map(String[] result, Context context) throws IOException, InterruptedException {
		
		if ( isFirstTime) {
			if ( HSearchLog.l.isInfoEnabled() ) HSearchLog.l.info("Map function started");
			isFirstTime = false;
		}

		if ( null != setupExp) throw setupExp;
		
		if ( isReportMapTime ) startTime = System.currentTimeMillis();
		
		processLine(result, context);
		
		if ( isReportMapTime ) {
			endTime = System.currentTimeMillis();

			if ( (endTime - startTime) > aMapTime) {
				HSearchLog.l.warn("Slow record processing in : " + 
					(endTime - startTime) + " (ms) \t" + 
					Runtime.getRuntime().totalMemory() + "\t" + 
					Runtime.getRuntime().maxMemory() + "\t" +
					Runtime.getRuntime().freeMemory() + "\t" + 
					slowRecordBuilder.append(result, '\t'));
			}
		}
		
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void processLine(String[] result, Context context) throws IOException, InterruptedException {
		
		/**
		 * Check if internal is available in schema (internalKey = -1) if available
		 * retrieve from the data else create a new one. 
		 */
		if(-1 == fm.internalKey){
			mergeId = createPartitionKey(result);
			
			/**
			 * Keep all merge ids as one row.
			 */
			if(! ids.containsKey(mergeId)){
				context.write(new TextPair(KVIndexer.MERGEKEY_ROW, EMPTY), new Text(mergeId));
			}

			//get incremental value for id
			if(ids.containsKey(mergeId)){
				Counter c = ids.get(mergeId);
				if(c.maxPointer <= c.seekPointer){
					c.maxPointer = createIncrementalValue(mergeId, mergeKeyCacheSize);
					if(c.maxPointer > Integer.MAX_VALUE)
						throw new IOException("Maximum limit reached please revisit your merge keys in the schema.");
					c.seekPointer = c.maxPointer - mergeKeyCacheSize;
					incrementalIdSeekPosition = c.seekPointer++;
				} else{
					incrementalIdSeekPosition = c.seekPointer++;
				}
			} else {
				long maxPointer = createIncrementalValue(mergeId, mergeKeyCacheSize);
				if(maxPointer > Integer.MAX_VALUE)
					throw new IOException("Maximum limit reached please revisit your merge keys in the schema.");
				long seekPointer = maxPointer - mergeKeyCacheSize;
				Counter c = new Counter(seekPointer, maxPointer);
				incrementalIdSeekPosition = c.seekPointer++;
				ids.put(mergeId, c);
			}
		} else {
			//retrieve from data
			String internalKey = result[fm.internalKey].trim();
			isFieldNull = (null == internalKey) ? true : internalKey.length() == 0;
			if(isFieldNull)
				return;
			incrementalIdSeekPosition = Integer.parseInt(internalKey);
			mergeId = result[fm.internalKey + 1];
			
			context.setStatus("I Key " + internalKey + " M Key : " + mergeId);
		}

		/*
		 * Add bucket number based on skew point
		 */
		if(fm.skewPoint > 0){
			int skewBucketNumber = (int)incrementalIdSeekPosition / fm.skewPoint;
			mergeId = mergeId + "_" + skewBucketNumber;					
		}

		for ( int neededIndex : neededPositions) {
			if(neededIndex < 0)continue;
			FieldMapping.Field fld = fm.sourceSeqWithField.get(neededIndex);

			if ( fld.isMergedKey ) continue;

			if ( fld.isDocIndex ) {
				boolean hasExpression = ( null == fld.expression) ? false : (fld.expression.trim().length() > 0 );
				if ( hasExpression ) {
					fldValue = evalExpression(fld.expression, result);
				} else {
					fldValue =  result[neededIndex];
				}
			} else {
				fldValue =  result[neededIndex];
			}

			isFieldNull =  (fldValue == null) ? true : (fldValue.length() == 0 );
			if ( fld.skipNull ) {
				if ( isFieldNull ) continue;
			} else {
				if (isFieldNull) fldValue = fld.defaultValue;
			}

			if(fld.isDocIndex) {
				if ( fld.isRepeatable) mapFreeTextBitset(fld, context);
				else mapFreeTextSet(fld, context);
			}

			if(fld.isStored) {
				boolean isEmpty = ( null == mergeId) ? true : (mergeId.length() == 0);
				rowKeyP1 = ( isEmpty) ? fld.name : mergeId + "_" + fld.name;
				appender.delete(0, appender.capacity());
				rowKeyP2 = appender.append(fld.getDataType()).append(KVIndexer.FIELD_SEPARATOR)
								   .append( fld.sourceSeq).toString();
				appender.delete(0, appender.capacity());
				rowVal = appender.append(incrementalIdSeekPosition).append(KVIndexer.FIELD_SEPARATOR).append(fldValue).toString();
				
				context.write(new TextPair(rowKeyP1, rowKeyP2), new Text(rowVal) );
			}

		}
	}

	public String createPartitionKey(String[] result){

		rowIdMap.clear();
		String fldValue = null;
		String rowId = EMPTY;

		for (int neededIndex : neededPositions) {
			if(neededIndex < 0)continue;
			FieldMapping.Field fld = fm.sourceSeqWithField.get(neededIndex);
			if(!fld.isMergedKey) continue;
			fldValue = result[neededIndex];
			if ( null == fldValue) fldValue = fld.defaultValue;
			else if (fldValue.trim().length() == 0  ) fldValue = fld.defaultValue;
			rowIdMap.put(fld.mergePosition, fldValue);
		}

		String[] megedKeyArr = new String[rowIdMap.size()];

		for (Integer mergePosition : rowIdMap.keySet()) {
			megedKeyArr[mergePosition] = rowIdMap.get(mergePosition);
		}

		boolean isFirst = true;
		for (int j = 0; j < megedKeyArr.length; j++) {
	
			if(isFirst)
				isFirst = false;
			else
				rowId = rowId + "_";
	
			rowId = rowId + megedKeyArr[j]; 
		}
		/**
		 * This is used for caching. So different schema with same field name may cause a conflict.
		 * Make it unique by adding the tablename at the starting
		 */
		if ( null == partName) {
			partName = fm.tableName.replaceAll("[^A-Za-z0-9]", EMPTY);
			if ( null == partName) partName = "p1";
			else partName = partName + "p1";
		}
		
		rowId = ( rowId.length() > 0 ) ? rowId : partName;
		return rowId;
	}

	public final String evalExpression(final String sourceNames, final String[] result){
		List<String> names = new ArrayList<String>();
		LineReaderUtil.fastSplit(names, sourceNames, PLUS);
		appender.delete(0, appender.capacity());
		String tempValue = null;
		boolean isEmpty = false;
		for (String name : names) {
			Field fld = fm.nameWithField.get(name);
			tempValue = result[fld.sourceSeq];
			isEmpty = (null == tempValue) ? true : tempValue.length() == 0;
			if(isEmpty)
				continue;
			appender.append(tempValue).append(" ");
		}

		return appender.toString();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void mapFreeTextSet(Field fld, Context context) throws IOException, InterruptedException{

		terms.clear();
		CharTermAttribute termAttribute = null;
		TokenStream stream = null;

		int wordhash;
		String wordhashStr;
		char firstChar;
		char lastChar;

		try {
			if(isFieldNull) return;
			
			Analyzer analyzer = AnalyzerFactory.getInstance().getAnalyzer(fld.name);
			stream = analyzer.tokenStream(fld.name, new StringReader(fldValue));
			termAttribute = stream .getAttribute(CharTermAttribute.class);

			while (stream .incrementToken()) {
				String termWord = termAttribute.toString();
				wordhash = Hashing.hash(termWord);
				wordhashStr = new Integer(wordhash).toString();
				firstChar = wordhashStr.charAt(0);
				lastChar = wordhashStr.charAt(wordhashStr.length() - 1);

				rowKeyP1 = mergeId + "_" + firstChar + "_" + lastChar;
				appender.delete(0, appender.capacity());
				rowKeyP2 = appender.append("text").append(KVIndexer.FIELD_SEPARATOR ).append(fld.sourceSeq).toString();
				appender.delete(0, appender.capacity());
				rowVal = appender.append(incrementalIdSeekPosition).append(KVIndexer.FIELD_SEPARATOR).append(wordhash).toString();
				
				context.write(new TextPair(rowKeyP1, rowKeyP2), new Text(rowVal));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	Set<Term> terms = new LinkedHashSet<Term>();

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public final void mapFreeTextBitset(final Field fld, final Context context) throws IOException, InterruptedException{

		terms.clear();
		CharTermAttribute termAttribute = null;
		TokenStream stream = null;
		try {
			if(isFieldNull) return;
			
			Analyzer analyzer = AnalyzerFactory.getInstance().getAnalyzer(fld.name);
			stream = analyzer.tokenStream(fld.name, new StringReader(fldValue));
			termAttribute = stream .getAttribute(CharTermAttribute.class);
			String last2 = null;
			String last1 = null;
			while (stream .incrementToken()) {
				String termWord = termAttribute.toString();

				if(0 == termWord.length())continue;
				
				appender.delete(0, appender.capacity());

				/**
				 * Row Key is mergeidFIELDwordhashStr
				 */
				boolean isEmpty = ( null == mergeId) ? true : (mergeId.length() == 0);
				String rowKeyPrefix = ( isEmpty) ? fld.name : mergeId + "_" + fld.name;

				rowKeyP1 = rowKeyPrefix + termWord;
				rowKeyP2 = appender.append("text").append(KVIndexer.FIELD_SEPARATOR )
								   .append(fld.sourceSeq).toString();

				appender.setLength(0);
				rowVal = appender.append(incrementalIdSeekPosition).toString();

				context.write(new TextPair(rowKeyP1, rowKeyP2), new Text(rowVal));

				if ( ! fld.isBiWord && !fld.isTriWord) continue;

				/**
				 * Do Three phrase word
				 */
				if ( null != last2) {
					appender.setLength(0);

					rowKeyP1 = appender.append(rowKeyPrefix).append(last2).append(' ').
							append(last1).append(' ').append(termWord).append(' ').append('*').toString();
					
					appender.setLength(0);
					rowKeyP2 = appender.append("text").append( KVIndexer.FIELD_SEPARATOR )
									   .append(fld.sourceSeq).toString();
					
					context.write(new TextPair(rowKeyP1, rowKeyP2), new Text(rowVal));
				}

				/**
				 * Do Two phrase word
				 */
				if ( null != last1) {
					
					appender.setLength(0);
					rowKeyP1 = appender.append(rowKeyPrefix).append(last1).append(' ')
										.append(termWord).append(' ').append('*').toString();
					
					appender.setLength(0);
					rowKeyP2 = appender.append("text").append( KVIndexer.FIELD_SEPARATOR )
									   .append(fld.sourceSeq).toString();
					
					context.write(new TextPair(rowKeyP1,rowKeyP2), new Text(rowVal));
				}

				last2 = last1;
				last1 = termWord;

			}
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Error While tokenizing : " + e.getMessage());
		} finally {
			try {
				if ( null != stream ) stream.close();
			} catch (Exception ex) {
				IdSearchLog.l.warn("Error during Tokenizer Stream closure");
			}
		}
	}    

	public long createIncrementalValue(String mergeID, int cacheSize) throws IOException{
		long id = 0;
		String row = mergeID + KVIndexer.INCREMENTAL_ROW;

		NV kvThisRow = new NV(kv.family, kv.name, LONG_0);
		RecordScalar scalar = new RecordScalar(row.getBytes(), kvThisRow);
		if(! HReader.exists(tableName, row.getBytes())){
			synchronized (KVMapperBase.class.getName()) {
				if( ! HReader.exists(tableName, row.getBytes())) {
					HWriter.getInstance(false).insertScalar(tableName, scalar);
				}
			}
		}
		id = HReader.idGenerationByAutoIncr(tableName, scalar, cacheSize);
		if ( HSearchLog.l.isInfoEnabled() )  HSearchLog.l.info("INFO > Next Ids:" + id);
		return id;
	}
}
