package readelement;

import guttmanlab.core.alignment.SmithWatermanAlignment;

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
	public boolean matchesFullString(String s) {
		if(getLength() != s.length()) {
			return false;
		}
		if(s.equalsIgnoreCase(seq)) {
			return true;
		}
		return AlignmentUtils.hammingDistanceAtMost(s, seq, maxNumMismatches, true);
	}

	@Override
	public String elementName() {
		return name;
	}

	@Override
	public boolean matchesSubstringNoGaps(String s, int startOnString) {
		return matchesFullString(s.substring(startOnString, startOnString + getLength()));
	}

	@Override
	public String getId() {
		return elementName();
	}

	@Override
	public MatchedElement matchedElement(String s) {
		if(matchesSubstringNoGaps(s, 0)) {
			return new MatchedElement(this, length);
		}
		jaligner.Alignment align = SmithWatermanAlignment.align(s, seq, SW_MATCH_SCORE, SW_MISMATCH_SCORE, SW_GAP_OPEN_PENALTY, SW_GAP_EXTEND_PENALTY);
		if(align.getStart1() != 0) return null; // Must match beginning of string
		int matches = align.getNumberOfMatches();
		int nonMatch = length - matches;
		if(nonMatch > maxNumMismatches) return null; //TODO is this how we want to count indels?
		int lengthOnSeq1 = align.getNumberOfMatches() + align.getGaps2(); //TODO is this right?
		return new MatchedElement(this, lengthOnSeq1);
	}

	@Override
	public boolean isRepeatable() {
		return false;
	}

	@Override
	public ReadSequenceElement getStopSignalForRepeatable() {
		return null;
	}

	
}
