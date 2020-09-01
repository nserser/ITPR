package RDR.RDR;

import java.util.Arrays;

public class Record {
	
	String[] content;
	
	

	public String[] getContent() {
		return content;
	}

	public void setContent(String[] content) {
		this.content = content;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(content);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Record other = (Record) obj;
		if (!Arrays.equals(content, other.content))
			return false;
		return true;
	}

	
}
