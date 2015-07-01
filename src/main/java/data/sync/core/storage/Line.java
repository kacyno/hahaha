
package data.sync.core.storage;

public interface Line {
	
	boolean addField(String field);
	
	boolean addField(String field, int index);
	
	String getField(int idx);
	
	String checkAndGetField(int idx);
	
	int getFieldNum();
	
	StringBuffer toStringBuffer(char separator);
	
	String toString(char separator);
	
	Line fromString(String lineStr, char separator);
	
	int length();
	
}
