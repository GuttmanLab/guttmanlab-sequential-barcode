package matcher;

import readelement.ReadSequenceElement;

/**
 * Class to store a ReadSequenceElement and where it matches on a read
 * @author prussell
 *
 */
public final class MatchedElement {
	
	private ReadSequenceElement element;
	private int lengthOnRead;
	private int matchStartOnRead;
	
	/**
	 * @param matchedElement The read sequence element that matches a read/sequence
	 * @param startPosOnRead Start position of match on read
	 * @param matchLengthOnRead Length of the match on the read
	 */
	public MatchedElement(ReadSequenceElement matchedElement, int startPosOnRead, int matchLengthOnRead) {
		element = matchedElement;
		lengthOnRead = matchLengthOnRead;
		matchStartOnRead = startPosOnRead;
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
	 * @return Start position of match on read
	 */
	public int getMatchStartPosOnRead() {
		return matchStartOnRead;
	}
	
	/**
	 * @return The "intended" sequence of the match, the matched element, not the matched part of the read
	 */
	public String getMatchedElementSequence() {
		return element.getSequence();
	}
	
}
