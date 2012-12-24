package com.bizosys.hsearch.index;

import com.oneline.ApplicationFault;
import com.oneline.SystemFault;
import com.oneline.util.Configuration;

public interface PipeIn {
	public PipeIn getInstance();
	public String getName();
	public void init(Configuration conf);
	public void visit(PipeInData data) throws ApplicationFault, SystemFault;
	public void commit(PipeInData data);
}
