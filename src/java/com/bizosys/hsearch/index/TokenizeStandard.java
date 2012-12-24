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
package com.bizosys.hsearch.index;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import com.bizosys.hsearch.idsearch.meta.DocMetaFilters;
import com.bizosys.hsearch.idsearch.meta.DocMetaTableRow;
import com.bizosys.hsearch.idsearch.table.TermTable;
import com.bizosys.hsearch.index.util.ContentField;
import com.bizosys.hsearch.index.util.ContentFieldReader;
import com.bizosys.hsearch.index.util.Document;
import com.bizosys.hsearch.index.util.LuceneConstants;
import com.bizosys.hsearch.index.util.TermStream;
import com.bizosys.hsearch.treetable.Cell2;
import com.bizosys.hsearch.treetable.Cell3;
import com.bizosys.hsearch.treetable.Cell4;
import com.bizosys.hsearch.treetable.Cell5;
import com.bizosys.hsearch.treetable.CellKeyValue;
import com.oneline.ApplicationFault;
import com.oneline.SystemFault;
import com.oneline.util.Configuration;

public class TokenizeStandard implements PipeIn {

	public TokenizeStandard() {
	}
	
	public PipeIn getInstance() {
		return this;
	}

	public String getName() {
		return "TokenizeStandard";
	}

	public void init(Configuration conf) {
	}

	public void visit(PipeInData data) throws ApplicationFault, SystemFault {

		data.processingDocFieldReaders = TokenizeBase.getReaders(data.processingDoc.fields);
    	if (null == data.processingDocFieldReaders) return;
		
		try {
			
    		Analyzer analyzer = new StandardAnalyzer(LuceneConstants.version);
	    	for (ContentFieldReader reader : data.processingDocFieldReaders ) {
	    		TokenStream stream = analyzer.tokenStream( new Integer(reader.fieldName).toString(), reader.reader);
	    		TermStream ts = new TermStream( reader.fieldName, stream, reader.weight); 
	    		data.processingDocTokenStreams.add(ts);
			}
	    	analyzer.close();
    	} catch (Exception ex) {
    		throw new SystemFault(ex);
    	}
	}

	@Override
	public void commit(PipeInData data) {
	}
	
	public static void main(String[] args) throws Exception {
		ContentField fld = new ContentField();
		fld.name = 98;
		fld.content = "Going cars carried Pramod Rao";
		fld.searchable = true;
		fld.keepOriginal = false;
		fld.weight = 11;

		PipeInData data = new PipeInData();
		data.processingDoc = new Document();
		data.processingDoc.meta = new DocMetaTableRow();
		data.processingDoc.meta.docId = 1;
		data.processingDoc.meta.filters = new DocMetaFilters( (byte) 0, 1, 1L, 1L) ;
		data.processingDoc.fields = new ArrayList<ContentField>();
		data.processingDoc.fields.add(fld);
		
		data.termsFromAllDocuments = new TermTable();

		System.out.println( "TokenizeStandard. START" );
		new TokenizeStandard().visit(data);
		System.out.println( "TokenizeStandard. END Total Streams :" + data.processingDocTokenStreams.size());
		
		new FilterStem().visit(data);
		//new FilterLowercase().visit(data);
		new ComputeTokens().visit(data);
		
		
		Map<Integer, Cell5<String, Integer, Integer, Integer, Float>> mapHashCodecs = data.termsFromAllDocuments.getTable().getMap();
		
		StringBuilder outputData = new StringBuilder();
		Iterator<Entry<Integer, Cell5<String, Integer, Integer, Integer, Float>>> hascodecItr = mapHashCodecs.entrySet().iterator();  
		while ( hascodecItr.hasNext() ) {
			Entry<Integer, Cell5<String, Integer, Integer, Integer, Float>> aHash = hascodecItr.next();
			Integer _hashCode = aHash.getKey();
			Cell5<String, Integer, Integer, Integer, Float> cell5 = aHash.getValue();
			Iterator<Entry<String, Cell4<Integer, Integer, Integer, Float>>> itemItr = 
					cell5.getMap().entrySet().iterator();  
			while ( itemItr.hasNext()) {
				Entry<String, Cell4<Integer, Integer, Integer, Float>> aTerm = itemItr.next();
				String _term = aTerm.getKey();
				Cell4<Integer, Integer, Integer, Float> cell4 = aTerm.getValue();
				Iterator<Entry<Integer, Cell3<Integer, Integer, Float>>> docItr = cell4.getMap().entrySet().iterator();
				while ( docItr.hasNext()) {

					Entry<Integer, Cell3<Integer, Integer, Float>> aDoc = docItr.next();
					Integer _doc = aDoc.getKey();
					Cell3<Integer, Integer, Float> cell3 = aDoc.getValue();
					
					Iterator<Entry<Integer, Cell2<Integer, Float>>> termtypeItr = cell3.getMap().entrySet().iterator();
					while ( termtypeItr.hasNext()) {
					
						Entry<Integer, Cell2<Integer, Float>> word = termtypeItr.next();
						Integer _wordtype = word.getKey();
						Cell2<Integer, Float> cell2 = word.getValue();
						
						for (CellKeyValue<Integer, Float> _word : cell2.getMap()) {
							outputData.append(_hashCode + "|" + _term + "|" + _doc + "|" + _wordtype + "|" + _word.getKey() + "|" + _word.getValue() + "\n");
						}
					}
				}
				
			}
		}
		System.out.println ( outputData.toString());
		
	}
	
}
