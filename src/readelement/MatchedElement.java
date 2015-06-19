package readelement;

/**
 * Class to store a ReadSequenceElement and where it matches on a read
 * @author prussell
 *
 */
public class MatchedElement {
	
	private ReadSequenceElement element;
	private int startPosOnRead;
	private int lengthOnRead;
	
	/**
	 * @param matchedElement The read sequence element that matches a read
	 * @param matchStartPosOnRead Start position of the match on the read
	 * @param matchLengthOnRead Length of the match on the read
	 */
	public MatchedElement(ReadSequenceElement matchedElement, int matchStartPosOnRead, int matchLengthOnRead) {
		element = matchedElement;
		startPosOnRead = matchStartPosOnRead;
		lengthOnRead = matchLengthOnRead;
	}
	
	/**
	 * @return The read sequence element that matches a read
	 */
	public ReadSequenceElement getMatchedElement() {
		return element;
	}
	
	/**
	 * @return Start position of the match on the read
	 */
	public int getMatchStartPosOnRead() {
		return startPosOnRead;
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
