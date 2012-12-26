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
package com.bizosys.hsearch.unstructured.tokenizer;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;

import com.bizosys.hsearch.idsearch.meta.DocMetaTableRow;
import com.bizosys.hsearch.idsearch.table.TermTable;
import com.bizosys.hsearch.unstructured.util.ContentFieldReader;
import com.bizosys.hsearch.unstructured.util.Term;
import com.bizosys.hsearch.unstructured.util.TermStream;
import com.oneline.ApplicationFault;
import com.oneline.SystemFault;
import com.oneline.util.Configuration;

/**
 * Tokenize text content
 * @author karan
 *
 */
public class ComputeTokens implements PipeIn {

	public PipeIn getInstance() {
		return this;
	}

	public String getName() {
		return "ComputeTokens";
	}

	public void init(Configuration conf) {
	}

	public void visit(PipeInData data) throws ApplicationFault, SystemFault {
		
		if ( null == data.termsFromAllDocuments) throw new ApplicationFault("No terms");
		if ( null == data.processingDoc.meta ) throw new ApplicationFault("No Meta");

		List<TermStream> streams = data.processingDocTokenStreams;
		if ( null != streams) {
			try {
				for (TermStream stream : streams) {
					System.out.println("Tokenizing");
					tokenize(data.processingDoc.meta, data.termsFromAllDocuments, stream);
					stream.stream.close();
					stream = null;
					System.out.println("Done");
				}
			} catch (IOException ex) {
				throw new SystemFault ("ComputeTokens : Tokenize Failed." , ex);
			} finally {
				streams.clear();
				cleanReaders(data.processingDocFieldReaders);
			}
		}
		
	}
	
	private void tokenize(DocMetaTableRow currentDocument, TermTable terms, TermStream ts) 
	throws ApplicationFault, IOException {
		
		if ( null == ts) return;
		TokenStream stream = ts.stream;
		if ( null == stream) return;
		
		if ( null == terms) {
			throw new ApplicationFault("TermTable is not initialized");
		}
		
		String token = null;
		int offset = 0;
		CharTermAttribute termA = (CharTermAttribute)stream.getAttribute(CharTermAttribute.class);
		OffsetAttribute offsetA = (OffsetAttribute) stream.getAttribute(OffsetAttribute.class);
		stream.reset();

		while ( stream.incrementToken()) {
			token = termA.toString();
			offset = offsetA.startOffset();
			Term term = new Term(
					currentDocument.docId, token, currentDocument.getFilter().documentType, ts.fieldName, ts.weight, offset);
			terms.addSearchData(term.toTermTableRow());
		}
		stream.close();
	}
	
	private void cleanReaders(List<ContentFieldReader> readers) {
		if ( null == readers) return;
		for (ContentFieldReader reader : readers) {
			try {
				if ( null != reader.reader ) reader.reader.close();
			} catch (IOException ex) {
				InpipeLog.l.warn("ComputeTokens:cleanReaders() reader closing failed", ex);
			}
		}
		readers.clear();
	}

	@Override
	public void commit(PipeInData data) {
	}
	
}
