package readelement;

import org.apache.commons.lang.builder.HashCodeBuilder;

public abstract class AbstractReadSequenceElement implements ReadSequenceElement {

	@Override
	public MatchedElement matchedElement(String s, int startPosOnString) {
		return matchedElement(s.substring(startPosOnString));
	}

	public boolean equals(Object o) {
		if(!o.getClass().equals(Barcode.class)) {
			return false;
		}
		Barcode other = (Barcode)o;
		return other.getId().equals(getId()) && other.getSequence().equals(getSequence());
	}
	
	public int hashCode() {
		HashCodeBuilder b = new HashCodeBuilder();
		b.append(getId());
		b.append(getSequence());
		return b.toHashCode();
	}

}
