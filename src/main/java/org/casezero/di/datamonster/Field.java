package org.casezero.di.datamonster;

public class Field {
    private int columnNumber;
    private String source;
    private String originalFieldName;
    private String normalizedFieldName;
    
    
	public int getColumnNumber() {
		return columnNumber;
	}
	public void setColumnNumber(int columnNumber) {
		this.columnNumber = columnNumber;
	}
	public String getSource() {
		return source;
	}
	public void setSource(String source) {
		this.source = source;
	}
	public String getOriginalFieldName() {
		return originalFieldName;
	}
	public void setOriginalFieldName(String originalFieldName) {
		this.originalFieldName = originalFieldName;
	}
	public String getNormalizedFieldName() {
		return normalizedFieldName;
	}
	public void setNormalizedFieldName(String normalizedFieldName) {
		this.normalizedFieldName = normalizedFieldName;
	}
    
    
}
