package readelement;

import java.util.Collection;
import java.util.HashSet;

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
	private Collection<String> matchedStrings;
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
		matchedStrings = new HashSet<String>();
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
		if(matchedStrings.contains(s)) {
			return true;
		}
		if(getLength() != s.length()) {
			return false;
		}
		if(s.equalsIgnoreCase(seq)) {
			matchedStrings.add(s);
			return true;
		}
		boolean rtrn = AlignmentUtils.hammingDistanceAtMost(s, seq, maxNumMismatches, true);
		if(rtrn) {
			matchedStrings.add(s);
		}
		return rtrn;
	}

	@Override
	public String elementName() {
		return name;
	}

	@Override
	public boolean matchesSubstringOf(String s, int startOnString) {
		return matchesFullString(s.substring(startOnString, startOnString + getLength()));
	}

	@Override
	public String getId() {
		return elementName();
	}

	@Override
	public MatchedElement matchedElement(String s) {
		if(!matchesFullString(s)) {
			return null;
		}
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

	
}
