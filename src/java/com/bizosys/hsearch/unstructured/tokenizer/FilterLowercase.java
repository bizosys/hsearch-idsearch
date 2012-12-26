package com.bizosys.hsearch.unstructured.tokenizer;

import java.util.List;

import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;

import com.bizosys.hsearch.unstructured.util.LuceneConstants;
import com.bizosys.hsearch.unstructured.util.TermStream;
import com.oneline.ApplicationFault;
import com.oneline.SystemFault;
import com.oneline.util.Configuration;

public class FilterLowercase implements PipeIn {

	@Override
	public PipeIn getInstance() {
		return this;
	}

	@Override
	public String getName() {
		return "FilterLowercase";
	}

	@Override
	public void init(Configuration conf) {
	}

	@Override
	public void visit(PipeInData data) throws ApplicationFault, SystemFault {
		if ( null == data) throw new ApplicationFault("No Data");

		if ( null == data.processingDocTokenStreams) throw new ApplicationFault("No tokenStreams");
		
		
		List<TermStream> streams = data.processingDocTokenStreams;
		if ( null == streams) return; //Allow for no bodies
		
		for (TermStream ts : streams) {
			TokenStream stream = ts.stream;
			if ( null == stream) continue;
			stream = new LowerCaseFilter(LuceneConstants.version, stream);
			ts.stream = stream;
		}
		return;
	}

	@Override
	public void commit(PipeInData data) {
	}

}
