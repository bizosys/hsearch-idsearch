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

import com.bizosys.hsearch.util.LineReaderUtil;

public class StructureKey {
	
	public static final String DEFAULT_FIELD_NAME = "[F";
	public static final String DEFAULT_PARTITION_KEY = "[P";
	private static final char STRUCTURE_KEY_SEPARATOR = '\t';
	
	public String partionKey = null;
	public int currentSkew = 0;
	public String fieldName = null;
	public String valueField = null;

	public StructureKey(){
		clear();
	}

	public final void clear() {
		this.partionKey  = DEFAULT_PARTITION_KEY;
		this.currentSkew = Integer.MIN_VALUE;
		this.fieldName   = DEFAULT_FIELD_NAME;
		this.valueField = "";
	}
	
	public StructureKey(final String partionKey, final String fieldName, 
			final String valueField, final int currentSkew) {
		this.partionKey = partionKey;
		this.currentSkew = currentSkew;
		this.fieldName = fieldName;
		this.valueField = valueField;
	}	
	
	public final void set(final String partionKey, final int currentSkew) {
		this.partionKey = partionKey;
		this.currentSkew = currentSkew;
	}	
	
	public final String getKey(final StringBuilder sb) {
		sb.setLength(0);
		if ( null != partionKey) partionKey = partionKey.replace(STRUCTURE_KEY_SEPARATOR, ' ');
		
		sb.append(partionKey).append(STRUCTURE_KEY_SEPARATOR);
		sb.append(currentSkew).append(STRUCTURE_KEY_SEPARATOR);
		sb.append(fieldName);
		boolean isEmptyValueField = (null == valueField) ? true : valueField.trim().length() == 0;
		if(isEmptyValueField) return sb.toString(); 
		else {
			sb.append(valueField);
			return sb.toString();
		}
	}

	@Override
	public String toString() {
		return getKey(new StringBuilder());
	}
	
	public static final StructureKey parseKey(final String structureKey) {
		StructureKey k = new StructureKey();
		String[] parts = new String[4];
		LineReaderUtil.fastSplit(parts, structureKey, STRUCTURE_KEY_SEPARATOR);
		k.partionKey = parts[0];
		k.currentSkew = Integer.parseInt(parts[1].trim());
		k.fieldName = parts[2];
		k.valueField = parts[3];
		return k;
	}

	public static final String replacePart(final String key, final String val) {
		return key.replace(DEFAULT_PARTITION_KEY, val);
	}
	
	public static final String replaceFldName(final String key, final String val) {
		return key.replace(DEFAULT_FIELD_NAME, val);
	}

	public static final String replaceSkew(final String key, final int val) {
		return key.replace(new Integer(Integer.MIN_VALUE).toString(), new Integer(val).toString());
	}
	
	public final StructureKey clone(final StructureKey another) {
		another.partionKey = this.partionKey;
		another.currentSkew = this.currentSkew;
		another.fieldName = this.fieldName;
		another.valueField = this.valueField;
		return another;
	}
}
