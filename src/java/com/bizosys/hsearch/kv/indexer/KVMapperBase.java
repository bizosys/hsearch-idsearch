/**
*    Copyright 2014, Bizosys Technologies Pvt Ltd
*
*    This software and all information contained herein is the property
*    of Bizosys Technologies.  Much of this information including ideas,
*    concepts, formulas, processes, data, know-how, techniques, and
*    the like, found herein is considered proprietary to Bizosys
*    Technologies, and may be covered by U.S., India and foreign patents or
*    patents pending, or protected under trade secret laws.
*    Any dissemination, disclosure, use, or reproduction of this
*    material for any reason inconsistent with the express purpose for
*    which it has been disclosed is strictly forbidden.
*
*                        Restricted Rights Legend
*                        ------------------------
*
*    Use, duplication, or disclosure by the Government is subject to
*    restrictions as set forth in paragraph (b)(3)(B) of the Rights in
*    Technical Data and Computer Software clause in DAR 7-104.9(a).
*/

package com.bizosys.hsearch.kv.indexer;

import java.io.IOException;
import java.io.StringReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.Term;

import com.bizosys.hsearch.byteutils.ByteUtil;
import com.bizosys.hsearch.byteutils.ISortedByte;
import com.bizosys.hsearch.byteutils.SortedBytesString;
import com.bizosys.hsearch.idsearch.util.IdSearchLog;
import com.bizosys.hsearch.kv.impl.FieldMapping;
import com.bizosys.hsearch.kv.impl.FieldMapping.Field;
import com.bizosys.hsearch.kv.indexer.HsearchCounterUtil.HsearchCounters;
import com.bizosys.hsearch.util.HSearchConfig;
import com.bizosys.hsearch.util.HSearchLog;
import com.bizosys.hsearch.util.LineReaderUtil;
import com.bizosys.unstructured.AnalyzerFactory;

public final class KVMapperBase {
	
	private static final class SizeCounter {
		public int size = 0;

		public SizeCounter(final int size) {
			this.size = size;
		}
		
		public final void increment(final int increment) {
			this.size += increment;
		}
	}

	private static final String EMPTY = "";
	private static final char PLUS = '+';
	
	/**
	 * Field Positions defined in schema
	 */
	private Set<Integer> neededPositions = null;  
	protected FieldMapping fm = null;
	private Map<Integer, Field> sourceSeqWithField = null;

	/**
	 * Concatination of multiple fields to form a partition key.
	 */
	protected StringBuilder appender = new StringBuilder();
	protected String tableName = null;
	private int skewPoint = 1000000;
	private boolean hasSkew = true;
	private int internalKeyIndex = -1;
	private int mergeIdIndex = -1;

	/**
	 * For each Ids, the Counter. This could grow huge.
	 */
	private final long aMapTime = HSearchConfig.getInstance().getConfiguration().getLong("report.amaptime.cutoff", 45000L);
	private final boolean isReportMapTime =  false; 
	
	protected long incrementalIdSeekPosition = 0;

	String fldValue = null;
	boolean isFieldNull = false;
	String mergeId = EMPTY;
	StructureKey strctureKey = new StructureKey();
	StructureKey clonedStructureKey = new StructureKey();
	StringBuilder keySb = new StringBuilder(64);
	
	String partName = null;

	long startTime = -1L;
	long endTime = -1L;
	
	boolean isFirstTime = true;
	boolean isFirstRowTimeStamp = true;
	Map<Integer, Boolean> timestampMap = new HashMap<Integer, Boolean>();
	LineReaderUtil slowRecordBuilder = new LineReaderUtil(); 
	
	IOException setupExp = null;
		
	SimpleDateFormat formatter_yyyyMMddHHmmss = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	Map<Integer, String> selectFlds = new HashMap<Integer, String>();
	Map<Integer, String> whereFlds = new HashMap<Integer, String>();
	
	ISortedByte<String> ser = SortedBytesString.getInstance();
	Map<String, KVSpillerBucket> keyAndValues = null;
	Map<String, SizeCounter> keyAndValSize = null;
	int spillSizeLimit = 0;
	int spillCountLimit = 0;
	
	long counterTotalMapRecords = 0;
	long counterTotalMapSize = 0;
	long counterIndexMapTime = 0;
	
	@SuppressWarnings("rawtypes")
	public void setup(Context context) throws IOException, InterruptedException {

		counterIndexMapTime = System.currentTimeMillis();
		
		Configuration conf = context.getConfiguration();

		try {
			String path = conf.get(KVIndexer.XML_FILE_PATH);
			fm = FieldMapping.getInstance(path);
			AnalyzerFactory.getInstance().init(fm);
			neededPositions = fm.sourceSeqWithField.keySet();

			this.tableName = fm.tableName;
			this.skewPoint = fm.skewPoint;
			this.hasSkew = (this.skewPoint > 0);
			this.sourceSeqWithField = fm.sourceSeqWithField;
			this.internalKeyIndex = fm.internalKey;
			this.mergeIdIndex = this.internalKeyIndex + 1;
						
			int cols = neededPositions.size();
			keyAndValues = new HashMap<String, KVSpillerBucket>(cols);
			keyAndValSize = new HashMap<String, SizeCounter>(cols);

			spillSizeLimit = 16777216 / cols;
			spillCountLimit = spillSizeLimit/16;
			
		} catch (Exception ex) {
			String errorMsg = "Map setup failure. ";
			setupExp = new IOException(errorMsg + ex.getMessage() , ex);
		}

	}
	
	@SuppressWarnings({ "rawtypes" })
	protected void map(final String[] result, final Context context) throws IOException, InterruptedException {
		
		if ( isFirstTime) {
			if ( HSearchLog.l.isInfoEnabled() ) HSearchLog.l.info("Map function started");
			if ( null != setupExp) throw setupExp;
		}

		counterTotalMapRecords++;
		for (String acol : result) {
			int size = (null == acol) ? 0 : acol.length();
			counterTotalMapSize += size; 
		}
		
		if ( isReportMapTime ) startTime = System.currentTimeMillis();
		
		try {
			
			processLine(isFirstTime, result, context);
			
		} catch (Exception ex) {
			String errorLine = "Error Line Data:" + 
				((null == result) ? "empty line" :new LineReaderUtil().append(result, '\n'));
			context.setStatus( errorLine);
			throw new IOException("Error :" + ex.getMessage() + "\n" + errorLine , ex );
		} finally {
			isFirstTime = false;
		}
		
		if ( isReportMapTime ) {
			endTime = System.currentTimeMillis();

			if ( (endTime - startTime) > aMapTime) {
				IdSearchLog.l.warn("Slow record processing in : " + 
					(endTime - startTime) + " (ms) \t" + 
					Runtime.getRuntime().totalMemory() + "\t" + 
					Runtime.getRuntime().maxMemory() + "\t" +
					Runtime.getRuntime().freeMemory() + "\t" + 
					slowRecordBuilder.append(result, '\t'));
			}
		}
		
	}
	
	@SuppressWarnings({ "rawtypes"})
	private final void processLine(boolean isFirstTime, final String[] result, final Context context) throws IOException, InterruptedException {
		
		strctureKey.clear();
		
		/**
		 * retrieve KEY from data as a COLUMN
		 */
		if(mergeIdIndex >= result.length){
			String msg = "Total columnn in row " + Arrays.asList(result).toString() + " is lesser than the intenalKey position given in schema [" 
						  + result.length + "<" + mergeIdIndex +"]";
			HSearchLog.l.warn(msg);
			throw new IOException(msg);
		}
		
		String internalKey = result[internalKeyIndex];
		isFieldNull = (null == internalKey) ? true : internalKey.length() == 0;
		if(isFieldNull) {
			String msg = "Generate key job run is not completed " +
					"since the internal key is null for the line :\n" + 
					Arrays.asList(result).toString();
			HSearchLog.l.warn(msg);
			throw new IOException(msg);
		}
		
		mergeId = result[this.mergeIdIndex];
		
		int skewBucketNumber = -1;
		int internalId = Integer.parseInt(internalKey);
		if ( this.hasSkew ) {
			skewBucketNumber = (int) (internalId / this.skewPoint);
			internalId = internalId - (skewBucketNumber * this.skewPoint);
		}
		
		strctureKey.set(mergeId, skewBucketNumber);

		int resultT = result.length;
		
		for ( int neededIndex : neededPositions) {
			if(neededIndex < 0)continue;
			if ( neededIndex >= resultT)  throw new IOException(
				Arrays.asList(result).toString() + "\tExpected Index : " + neededIndex + " Actual " + result.length);

			FieldMapping.Field fld = this.sourceSeqWithField.get(neededIndex);
			
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

			isFieldNull =  (fldValue == null) ? true : (fldValue.trim().length() == 0 );
			if(isFieldNull){
				if ( fld.skipNull ) continue;
				else fldValue = fld.defaultValue;
			}

			strctureKey.fieldName = Integer.toString(neededIndex);
			if(fld.isDocIndex) {
				strctureKey.clone(clonedStructureKey);
				mapFreeTextBitset(fld, clonedStructureKey, context, internalId);
			}

			if(fld.isStored)
				appender.setLength(0);
				String replacedWithFldS = strctureKey.getKey(appender);
				bufferWritter(replacedWithFldS, fld.getDataTypeCode(), internalId, fldValue, context);
		}
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private final void bufferWritter(final String key, final byte datatype, final int internalKey, 
			final String value, final Context context) throws IOException, InterruptedException {
		
		try{
			if ( keyAndValues.containsKey(key) ) {

				KVSpillerBucket valL = keyAndValues.get(key);
				
				int size = valL.add(internalKey, value);
				SizeCounter existingSize = keyAndValSize.get(key);
				existingSize.increment(size);

				boolean spillDuetoSize = existingSize.size >= spillSizeLimit;
				boolean spillDuetoCount = valL.size() >= spillCountLimit;
				
				if ( spillDuetoSize || spillDuetoCount ) {
					byte[] bytes = valL.toBytes();
					existingSize.size = 0;
					if(null == bytes) return;
					BytesWritable bw = new BytesWritable(bytes);
					context.write(new Text(key), bw);
				}

			} else {
				KVSpillerBucket valL = new KVSpillerBucket(datatype);
				int size = valL.add(internalKey, value);
				keyAndValues.put(key, valL);
				SizeCounter sizeC = new SizeCounter(size);
				keyAndValSize.put(key, sizeC);
			}
			
		} catch (Exception e) {
			throw new IOException("KVMapperBase=>Error processing for Key [" + key + "], internal key [" + internalKey + "] and value [" + value + "]", e);
		}
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


	Set<Term> terms = new LinkedHashSet<Term>();

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public final void mapFreeTextBitset(final Field fld, StructureKey clonedKeyWithFld, final Context context, int internalId) throws IOException, InterruptedException{

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
			
			BytesWritable idBytes = new BytesWritable(ByteUtil.toBytes(internalId));
			
			while (stream .incrementToken()) {
				String termWord = termAttribute.toString().trim();

				if(0 == termWord.length())continue;
				clonedKeyWithFld.valueField = termWord;
				String newKey = clonedKeyWithFld.getKey(keySb);
				context.write(new Text(newKey), idBytes);

				if ( ! fld.isBiWord && !fld.isTriWord) continue;

				/**
				 * Do Three phrase word
				 */
				if ( null != last2) {
					appender.setLength(0);					
					newKey = appender.append(last2).append(' ')
					 .append(last1).append(' ')
					 .append(termWord).append("*")
					 .toString();
					clonedKeyWithFld.valueField = newKey;
					appender.setLength(0);
					newKey = clonedKeyWithFld.getKey(appender);
					context.write(new Text(newKey), idBytes);
				}

				/**
				 * Do Two phrase word
				 */
				if ( null != last1) {
					
					appender.setLength(0);
					newKey = appender.append(last1).append(' ')
					 .append(termWord).append("*")
					 .toString();
					clonedKeyWithFld.valueField = newKey;
					appender.setLength(0);
					newKey = clonedKeyWithFld.getKey(appender);
					context.write(new Text(newKey), idBytes);
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
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void cleanup(Context context) throws IOException, InterruptedException {

		context.getCounter(HsearchCounters.Total_Input_Records).increment(this.counterTotalMapRecords);
		context.getCounter(HsearchCounters.Total_Input_Bytes).increment(counterTotalMapSize);
		long timeTaken = (System.currentTimeMillis() - this.counterIndexMapTime);
		context.getCounter(HsearchCounters.Index_Map_Time).increment(timeTaken);
		
		if ( null != keyAndValues) {
			for (String key : keyAndValues.keySet() ) {

				KVSpillerBucket valL = keyAndValues.get(key);
				if ( valL.size() == 0 ) continue;
				
				byte[] bytes = valL.toBytes();
				int bytesLen = ( null == bytes) ? 0 : bytes.length;;
				if ( 0 == bytesLen) {
					throw new IOException(
						"Mapper cleanup, error in creating bytes. Serialized to 0 bytes serializing " + 
						valL.size() + "\n" + key);
				}
				valL.clear();
				BytesWritable bw = new BytesWritable();
				bw.set(bytes,0,bytes.length);
				context.write(new Text(key), bw);

			}
			keyAndValSize.clear();
			keyAndValues.clear();			
		}

	}
	
}
