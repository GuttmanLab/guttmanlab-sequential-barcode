package readelement;

import java.util.HashMap;
import java.util.Map;

import matcher.MatchedElement;

import org.apache.log4j.Logger;


/**
 * A fixed sequence that should be the same in all reads
 * @author prussell
 *
 */
public final class FixedSequence extends AbstractReadSequenceElement {
	
	private String seq;
	private int maxNumMismatches;
	private String name;
	public static Logger logger = Logger.getLogger(FixedSequence.class.getName());
	private int length;

	/**
	 * @param fixedSeqName Name of fixed sequence
	 * @param sequence Nucleotide sequence
	 * @param maxMismatches Max number of mismatches when identifying this sequence in a read
	 */
	public FixedSequence(String fixedSeqName, String sequence, int maxMismatches) {
		seq = sequence;
		length = seq.length();
		name = fixedSeqName;
		maxNumMismatches = maxMismatches;
	}
	
	@Override
	public int getLength() {
		return length;
	}

	/**
	 * Get the sequence
	 * @return The nucleotide sequence
	 */
	public String getSequence() {
		return seq;
	}

	@Override
	public String getId() {
		return elementName();
	}

	@Override
	public MatchedElement matchedElement(String s) {
		throw new UnsupportedOperationException("NA");
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
	public Map<String, ReadSequenceElement> sequenceToElement() {
		Map<String, ReadSequenceElement> rtrn = new HashMap<String, ReadSequenceElement>();
		rtrn.put(seq, this);
		return rtrn;
	}

	@Override
	public int minMatch() {
		return length - maxNumMismatches;
	}

	@Override
	public int maxLevenshteinDist() {
		return maxNumMismatches; // TODO should there be separate parameters?
	}

	@Override
	public String elementName() {
		return name;
	}

	
}
