package readelement;

/**
 * Class to store a ReadSequenceElement and where it matches on a read
 * Match must start at position 0
 * @author prussell
 *
 */
public class MatchedElement {
	
	private ReadSequenceElement element;
	private int lengthOnRead;
	
	/**
	 * @param matchedElement The read sequence element that matches a read/sequence starting at position 0
	 * @param matchLengthOnRead Length of the match on the read
	 */
	public MatchedElement(ReadSequenceElement matchedElement, int matchLengthOnRead) {
		element = matchedElement;
		lengthOnRead = matchLengthOnRead;
	}
	
	/**
	 * @return The read sequence element that matches a read
	 */
	public ReadSequenceElement getMatchedElement() {
		return element;
	}
	
	/**
	 * @return Length of the match on the read
	 */
	public int getMatchLengthOnRead() {
		return lengthOnRead;
	}
	
	/**
	 * @return The "intended" sequence of the match, the matched element, not the matched part of the read
	 */
	public String getMatchedElementSequence() {
		return element.getSequence();
	}
	
}
