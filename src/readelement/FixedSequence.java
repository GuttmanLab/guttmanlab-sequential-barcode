package readelement;

import guttmanlab.core.alignment.SmithWatermanAlignment;

import java.util.HashMap;
import java.util.Map;

//import guttmanlab.core.alignment.SmithWatermanAlignment;






import org.apache.log4j.Logger;

import nextgen.core.utils.AlignmentUtils;

/**
 * A fixed sequence that should be the same in all reads
 * @author prussell
 *
 */
public class FixedSequence extends AbstractReadSequenceElement {
	
	private String seq;
	private int maxNumMismatches;
	private String name;
	public static Logger logger = Logger.getLogger(FixedSequence.class.getName());
	private int length;
	// Smith waterman parameters
	private static float SW_MATCH_SCORE = 5;
	private static float SW_MISMATCH_SCORE = -4;
	private static float SW_GAP_OPEN_PENALTY = 8;
	private static float SW_GAP_EXTEND_PENALTY = 2;

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
