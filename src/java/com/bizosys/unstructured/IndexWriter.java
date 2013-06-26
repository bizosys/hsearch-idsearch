/*
* Copyright 2010 Bizosys Technologies Limited
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

package com.bizosys.unstructured;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.CorruptIndexException;

import com.bizosys.hsearch.byteutils.SortedBytesInteger;
import com.bizosys.hsearch.hbase.HWriter;
import com.bizosys.hsearch.hbase.NV;
import com.bizosys.hsearch.hbase.RecordScalar;
import com.bizosys.hsearch.treetable.client.partition.IPartition;
import com.bizosys.hsearch.treetable.storage.HBaseTableSchemaDefn;
import com.bizosys.hsearch.treetable.unstructured.IIndexFrequencyTable;
import com.bizosys.hsearch.treetable.unstructured.IIndexOffsetTable;
import com.bizosys.hsearch.treetable.unstructured.IIndexPositionsTable;
import com.bizosys.hsearch.util.HSearchLog;
import com.bizosys.hsearch.util.Hashing;


public class IndexWriter {

	private AnalyzerFactory analyzers = null;
	private static String unknownDocumentType = "-";
	
	private static final int FREQUENCY_TABLE = 0;
	private static final int OFFSET_TABLE = 1;
	private static final int POSITION_TABLE = 2;
	private int tableType = -1;
	
	static boolean INFO_ENABLED = HSearchLog.l.isInfoEnabled();
	
	private List<IndexRow> cachedIndex = new ArrayList<IndexWriter.IndexRow>();
	
	private IIndexFrequencyTable tableFrequency = null;
	private IIndexOffsetTable tableOffset = null;
	private IIndexPositionsTable tablePositions = null;
	
	SearchConfiguration sConf = null;	
	
	private IndexWriter() throws InstantiationException {
		sConf = SearchConfiguration.getInstance();
	}
	
	public IndexWriter(IIndexFrequencyTable tableFrequency) throws InstantiationException {
		this();
		this.tableFrequency = tableFrequency;
		tableType = FREQUENCY_TABLE;
	}
	
	public IndexWriter(IIndexOffsetTable tableOffset) throws InstantiationException {
		this();
		this.tableOffset = tableOffset;
		tableType = OFFSET_TABLE;
	}

	public IndexWriter(IIndexPositionsTable tablePosition) throws InstantiationException {
		this();
		this.tablePositions = tablePosition;
		tableType = POSITION_TABLE;
	}
	
	public byte[] toBytes() throws IOException {
		switch (tableType) {
			case FREQUENCY_TABLE :
				for (IndexRow row : this.cachedIndex) {
					int wordHash = row.hashCode(); 
					this.tableFrequency.put( row.docType, row.fieldType, 
							wordHash, row.docId, setPayloadWithOccurance(
								row.docType, row.fieldType,wordHash, row.docId,  row.occurance));
				}
				return this.tableFrequency.toBytes();

			case OFFSET_TABLE :
				for (IndexRow row : this.cachedIndex) {
					byte[] offsetB = SortedBytesInteger.getInstance().toBytes(row.offsetL);
					this.tableOffset.put( row.docType, row.fieldType, 
						row.hashCode(), row.docId, setPayloadWithOffsets(row.docId, offsetB));
				}
				return this.tableOffset.toBytes();
		
			case POSITION_TABLE :
				for (IndexRow row : this.cachedIndex) {
					byte[] positionsB = SortedBytesInteger.getInstance().toBytes(row.positionL);
					this.tablePositions.put( row.docType, row.fieldType, 
						row.hashCode(), row.docId, setPayloadWithPositions(row.docId, positionsB));
				}
				return this.tablePositions.toBytes();
			
			default:
				throw new IOException("Unknown Index Type");
		}
	}
	
	public byte[] toBytesOnUnique() throws IOException {
		Set<String> uniqueRows = new HashSet<String>();
		StringBuilder sb = null;
		
		switch (tableType) {
			case FREQUENCY_TABLE :
				for (IndexRow row : this.cachedIndex) {
					int wordHash = row.hashCode(); 
					if ( null == sb ) sb = new StringBuilder(1024);
					else sb.delete(0, sb.capacity());
					sb.append(row.docType).append('\t').append(row.fieldType).append('\t').append(wordHash).append('\t').append(row.docId).append('\t').append(row.occurance);
					String uniqueId = sb.toString();
					if ( uniqueRows.contains(uniqueId) ) continue;
					else {
						uniqueRows.add(uniqueId);
						this.tableFrequency.put( row.docType, row.fieldType,wordHash, row.docId,setPayloadWithOccurance( 
							row.docType, row.fieldType, wordHash, row.docId,  row.occurance));
					}
					
				}
				byte[] ser = this.tableFrequency.toBytes();
				uniqueRows.clear();
				return ser;

			case OFFSET_TABLE :
				for (IndexRow row : this.cachedIndex) {
					byte[] offsetB = SortedBytesInteger.getInstance().toBytes(row.offsetL);
					this.tableOffset.put( row.docType, row.fieldType, 
						row.hashCode(), row.docId, setPayloadWithOffsets(row.docId, offsetB));
				}
				return this.tableOffset.toBytes();
		
			case POSITION_TABLE :
				for (IndexRow row : this.cachedIndex) {
					byte[] positionsB = SortedBytesInteger.getInstance().toBytes(row.positionL);
					this.tablePositions.put( row.docType, row.fieldType, 
						row.hashCode(), row.docId, setPayloadWithPositions(row.docId, positionsB));
				}
				return this.tablePositions.toBytes();
			
			default:
				throw new IOException("Unknown Index Type");
		}
	}	
	
	public int setPayloadWithOccurance(int docType, int fieldType, int wordHash, int docId, int occurance) {
		return occurance;
	}
	
	public byte[] setPayloadWithPositions(int docId, byte[] positionsB) {
		return positionsB;
	}

	public byte[] setPayloadWithOffsets(int docId, byte[] offsetB) {
		return offsetB;
	}

	public void close() throws IOException {
		
		
		switch (tableType) {
			case FREQUENCY_TABLE :
				this.tableFrequency.clear();
				break;

			case OFFSET_TABLE :
				this.tableOffset.clear();
				break;
		
			case POSITION_TABLE :
				this.tablePositions.clear();
				break;
			
			default:
				throw new IOException("Unknown Index Type");
		}
		
		if ( null != analyzers) {
			analyzers.close();
		}
	}	
	
	public void addDocument(int docId, Document doc) throws IOException, InstantiationException {
		addDocument(docId, doc, unknownDocumentType);
	}

	public void addDocument(int docId, Document doc, String documentType) throws IOException, InstantiationException {
		this.analyzers = new AnalyzerFactory();
		this.analyzers.setDefault();
		addDocument(docId, doc, documentType, analyzers);
	}

	public void addDocument(int docId, Document doc, String documentType, AnalyzerFactory analyzer) throws CorruptIndexException, IOException, InstantiationException {
		Map<String, IndexRow> uniqueRows = new HashMap<String, IndexWriter.IndexRow>();
		addDocument(docId, doc, documentType, analyzer, uniqueRows ); 
	}	

	public void addDocument(int docId, Document doc, String documentType, 
		AnalyzerFactory analyzers, Map<String, IndexRow> uniqueTokens ) throws CorruptIndexException, IOException, InstantiationException {

		this.analyzers = analyzers;

		int docType = sConf.getDocumentTypeCodes().getCode(documentType);
		
		for (Fieldable field : doc.getFields() ) {
    		uniqueTokens.clear();
    		int fieldType = sConf.getFieldTypeCodes().getCode(field.name());
    		
    		if ( field.isTokenized()) {
        		StringReader sr = new StringReader(field.stringValue());
        		TokenStream stream = analyzers.getAnalyzer(documentType, field.name()).tokenStream(field.name(), sr);
        		tokenize(stream, docId, docType , fieldType, uniqueTokens);
        		sr.close();
    		} else {
    			cachedIndex.add(new IndexRow(docId, field.stringValue(), docType, fieldType, 0, 0));
    		}
    		
		}
		
		if ( null != uniqueTokens)  uniqueTokens.clear();
	}
	
	public void commit(String mergeId, String indexName) throws IOException {
		
		HBaseTableSchemaDefn schema = HBaseTableSchemaDefn.getInstance();
		
		String tableName = schema.tableName;
		if ( !schema.columnPartions.containsKey(indexName)) {
			throw new IOException("Unable to find partion points for " + indexName + ". Please initialize schema");
		}
		
		Map<String, Map<Character, List<IndexRow>>> partitionCells = new HashMap<String, 
				 Map<Character, List<IndexRow>>> (schema.columnPartions.size());
		
		segregateOnFamilyColumn(indexName, schema, partitionCells);
		
		for (String family : partitionCells.keySet()) {
			
			Map<Character, List<IndexRow>> cols = partitionCells.get(family);
			
			for ( Character column : cols.keySet()) {
				
				List<IndexRow> rows = cols.get(column);
				byte[] data = null;
				
				data = getBytes(rows, data);
				
				byte[] colNameBytes = new String( new char[] {column} ).getBytes();
				
				RecordScalar mergedTable = new RecordScalar(
						mergeId.getBytes(), 
						new NV(family.getBytes(), colNameBytes , data ) ) ;
				
				List<RecordScalar> records = new ArrayList<RecordScalar>(1);
		    	records.add(mergedTable);
		    	HWriter.getInstance(true).insertScalar(tableName, records);				
			}
		}
	}

	private final byte[] getBytes(List<IndexRow> rows, byte[] data) throws IOException {
		switch (tableType) {
		
			case FREQUENCY_TABLE :
				for (IndexRow row : rows) {
					this.tableFrequency.put( row.docType, row.fieldType, 
							row.hashCode(), row.docId, row.occurance);
				}
				data = this.toBytes();
				if ( INFO_ENABLED )  HSearchLog.l.info("Total rows in frequency table:\t" + rows.size());
				this.tableFrequency.clear();
				break;

			case POSITION_TABLE  :
				for (IndexRow row : rows) {
					byte[] positionsB = SortedBytesInteger.getInstance().toBytes(row.positionL);
					this.tablePositions.put( row.docType, row.fieldType, 
						row.hashCode(), row.docId, positionsB);
				}
				data = this.toBytes();
				if ( INFO_ENABLED )  HSearchLog.l.info("Total rows in position table:\t" + rows.size());
				this.tablePositions.clear();
				break;
		
			case OFFSET_TABLE:
				for (IndexRow row : rows) {
					byte[] offsetB = SortedBytesInteger.getInstance().toBytes(row.offsetL);
					this.tableOffset.put( row.docType, row.fieldType, 
						row.hashCode(), row.docId, offsetB);
				}
				data = this.toBytes();
				if ( INFO_ENABLED )  HSearchLog.l.info("Total rows in offset table:\t" + rows.size());
				this.tableOffset.clear();
				break;
		
			default:
				throw new IOException("Unknown Index Type");		
		}
		return data;
	}

	public void segregateOnFamilyColumn(String field,
			HBaseTableSchemaDefn schema,
			Map<String, Map<Character, List<IndexRow>>> partitionCells)
			throws IOException {
		@SuppressWarnings("rawtypes")
		IPartition partitions = schema.columnPartions.get(field);
		
		for (IndexRow row : this.cachedIndex) {
			
			String token = row.token;
			
			@SuppressWarnings("unchecked")
			String family = partitions.getColumnFamily( 
				new Integer(Hashing.hash(token)).toString());
			char colName = HBaseTableSchemaDefn.getColumnName(Hashing.hash(token));
			
			Map<Character, List<IndexRow>> familyMap = null;
			
			if ( partitionCells.containsKey(family)) {
				familyMap = partitionCells.get(family);
			} else {
				familyMap = new HashMap<Character, List<IndexRow>>();
				partitionCells.put(family,familyMap);
			}
			
			List<IndexRow> rows = null;
			if ( familyMap.containsKey(colName)) {
				rows = familyMap.get(colName);
			} else {
				rows = new ArrayList<IndexWriter.IndexRow>();
				familyMap.put(colName,rows);
			}
			
			rows.add(row);
		}
	}

	/**
	 * Find the last offset.
	 * Find each term offset
	 * 
	 * @param stream
	 * @param docId
	 * @param docType
	 * @param fieldType
	 * @param fieldBoost
	 * @param codecs
	 * @param uniqueTokens
	 * @throws IOException
	 */
	private final void tokenize(TokenStream stream, int docId, int docType, 
			int fieldType, Map<String, IndexRow> uniqueTokens ) throws IOException {
		
		String token = null;
		int curoffset = 0;
		int lastoffset = 0;
		int position = -1;

		StringBuilder sb = new StringBuilder();
		CharTermAttribute termA = (CharTermAttribute)stream.getAttribute(CharTermAttribute.class);
		OffsetAttribute offsetA = (OffsetAttribute) stream.getAttribute(OffsetAttribute.class);
		
		while ( stream.incrementToken()) {
			
			
			token = termA.toString();
			curoffset = offsetA.endOffset();
			
			if ( lastoffset != curoffset) position++;
			lastoffset = curoffset;
			
			String key = IndexRow.generateKey(sb, docId, token, docType, fieldType);
			sb.delete(0, sb.capacity());
			
			if (uniqueTokens.containsKey(key) ) {
				IndexRow existingRow = uniqueTokens.get(key);
				existingRow.set(curoffset, position);
				existingRow.occurance++;
			} else {
				IndexRow row = new IndexRow(docId, token,docType, fieldType, curoffset, position);
				uniqueTokens.put(key, row);
			}
		}
		stream.end();
		stream.close();
		
		for (IndexRow row : uniqueTokens.values()) cachedIndex.add(row);
	}	
	
	private static class IndexRow {
		
		public int docId;
		public String token;
		public int docType;
		public int fieldType;
		public List<Integer> offsetL = new ArrayList<Integer>();
		public List<Integer> positionL = new ArrayList<Integer>();
		public int occurance = 1;

		public IndexRow(int docId, String token, int docType, int fieldType, int offset, int position)  {
			this(docId, token, docType, fieldType);
			set ( offset, position);
		}
		
		public IndexRow(int docId, String token, int docType, int fieldType)  {
			this.docId = docId;
			this.token = token;
			this.docType = docType;
			this.fieldType = fieldType;
		}
		
		public void set ( int offset, int position) {
			this.offsetL.add(offset);
			this.positionL.add(position);
		}
		
		@Override
		public int hashCode() {
			return Hashing.hash(token);
		}
		
		public static String generateKey(StringBuilder sb, int docId, String token, int docType, int fieldType) {
			sb.append(docType).append('|').append(token).append('|').append(docId).append('|').append(fieldType);
			return sb.toString();
			
		}
	}
}


/**
double occuranceBoost = Math.log10(row.occurance);
if ( occuranceBoost > 1 ) occuranceBoost = 1;
double locationBoost = ( lastOffset - row.offset)/ lastOffset;
float finalBoost = (float) ( row.boost + occuranceBoost + locationBoost );
*/

