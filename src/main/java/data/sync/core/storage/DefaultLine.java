
package data.sync.core.storage;


public class DefaultLine implements Line {
	private String[] fieldList;

	private int length = 0;

	private int fieldNum = 0;

	public DefaultLine() {
		this.fieldList = new String[128];
	}

	public void clear() {
		length = 0;
		fieldNum = 0;
	}

	@Override
	public int length() {
		return length;
	}

	@Override
	public boolean addField(String field) {
		fieldList[fieldNum] = field;
		fieldNum++;
		if (field != null)
			length += field.length();
		return true;
	}

	@Override
	public boolean addField(String field, int index) {
		fieldList[index] = field;
		if (fieldNum < index + 1)
			fieldNum = index + 1;
		if (field != null)
			length += field.length();
		return true;
	}

	@Override
	public int getFieldNum() {
		return fieldNum;
	}

	@Override
	public String getField(int idx) {
		return fieldList[idx];
	}
	
	public String checkAndGetField(int idx) {
		if (idx < 0 ||
				idx >= fieldNum) {
			return null;
		}
		return fieldList[idx];
	}

	@Override
	public StringBuffer toStringBuffer(char separator) {
		StringBuffer tmp = new StringBuffer();
		for (int i = 0; i < fieldNum-1; i++) {
			tmp.append(fieldList[i]).append(separator);
		}
		tmp.append(fieldList[fieldNum-1]);
		return tmp;
	}
	
	@Override
	public String toString(char separator) {
		return this.toStringBuffer(separator).toString();
	}
	@Override
	public Line fromString(String lineStr, char separator) {
		return null;
	}
}
