package com.bizosys.hsearch.index;

import java.util.List;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardFilter;

import com.bizosys.hsearch.index.util.LuceneConstants;
import com.bizosys.hsearch.index.util.TermStream;
import com.oneline.ApplicationFault;
import com.oneline.SystemFault;
import com.oneline.util.Configuration;

public class FilterStandard implements PipeIn {

	public FilterStandard(){}
	
	public PipeIn getInstance() {
		return this;
	}

	public String getName() {
		return "Filter Standard";
	}

	public void init(Configuration conf) {
	}

	public void visit(PipeInData data) throws ApplicationFault, SystemFault 
	{
		if ( null == data) throw new ApplicationFault("No Data");
		if ( null == data.processingDocTokenStreams) throw new ApplicationFault("No Token Streams");
		
		List<TermStream> streams = data.processingDocTokenStreams;
		
		for (TermStream ts : streams) {
			TokenStream stream = ts.stream;
			if ( null == stream) continue;
			stream = new StandardFilter(LuceneConstants.version, stream);
			ts.stream = stream;
		}
	}

	public void commit(PipeInData data) {
	}
}
