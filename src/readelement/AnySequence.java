package readelement;

import java.util.Map;

import matcher.MatchedElement;

import org.apache.log4j.Logger;

/**
 * The notion of an endogenous sequence of a particular length
 * Does not store an actual nucleotide sequence
 * @author prussell
 *
 */
public final class AnySequence extends AbstractReadSequenceElement {
	
	private int length;
	public static Logger logger = Logger.getLogger(AnySequence.class.getName());

	/**
	 * @param len The sequence length
	 */
	public AnySequence(int len) {
		length = len;
	}
	
	@Override
	public int getLength() {
		return length;
	}

	@Override
	public String elementName() {
		return "endogenous_sequence";
	}

	@Override
	public String getId() {
		return null;
	}

	@Override
	public MatchedElement matchedElement(String s) {
		if(s.length() != length) {return null;}
		return new MatchedElement(this, 0, length);
	}

	@Override
	public boolean isRepeatable() {
		return false;
	}

	@Override
	public ReadSequenceElement getStopSignalForRepeatable() {
		return null;
	}

	@Override
	public String getSequence() {
		return null;
	}

	@Override
	public Map<String, ReadSequenceElement> sequenceToElement() {
		throw new UnsupportedOperationException("NA");
	}

	@Override
	public int minMatch() {
		return 0;
	}

	@Override
	public int maxLevenshteinDist() {
		throw new UnsupportedOperationException("NA");
	}

}
