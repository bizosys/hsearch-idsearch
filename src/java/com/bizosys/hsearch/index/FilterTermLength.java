package com.bizosys.hsearch.index;

import java.util.List;

import org.apache.lucene.analysis.LengthFilter;
import org.apache.lucene.analysis.TokenStream;

import com.oneline.ApplicationFault;
import com.oneline.SystemFault;
import com.oneline.util.Configuration;
import com.bizosys.hsearch.index.util.TermStream;

public class FilterTermLength implements PipeIn {

	public int minCharCutoff = 2;
	public int maxCharCutoff = 200;

	public FilterTermLength() {}
	
	public PipeIn getInstance() {
		return this;
	}

	public String getName() {
		return "FilterTermLength";
	}

	public void init(Configuration conf) 
	{
		this.minCharCutoff = conf.getInt("word.cutoff.minimum", 2);
		this.maxCharCutoff = conf.getInt("word.cutoff.maximum", 200);
	}

	public void visit(PipeInData data) throws ApplicationFault, SystemFault 
	{
		if ( null == data) throw new ApplicationFault("No data");
		if ( null == data.processingDocTokenStreams)  throw new ApplicationFault("No TokenStreams");
		
		List<TermStream> streams = data.processingDocTokenStreams;
		
		for (TermStream ts : streams) 
		{
			TokenStream stream = ts.stream;
			if ( null == stream) continue;
			stream = new LengthFilter(true, stream, minCharCutoff, maxCharCutoff);
			ts.stream = stream;
		}
	}

	public void commit(PipeInData data) {
	}
}
