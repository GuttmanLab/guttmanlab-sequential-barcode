package readelement;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

/**
 * A collection of possibilities of fixed sequences that indicate something about the identity of the fragment
 * For example, an adapter that indicates whether the fragment is DNA or RNA
 * @author prussell
 *
 */
public class Switch implements ReadSequenceElement {
	
	protected Collection<FixedSequence> fixedSequences;
	private int length;
	
	/**
	 * @param fixedSeqs The fixed sequence options for the read element
	 */
	public Switch(Collection<FixedSequence> fixedSeqs) {
		Collection<String> seqs = new HashSet<String>();
		Collection<String> names = new HashSet<String>();
		Iterator<FixedSequence> iter = fixedSeqs.iterator();
		FixedSequence first = iter.next();
		seqs.add(first.getSequence());
		names.add(first.getId());
		length = first.getLength();
		while(iter.hasNext()) {
			FixedSequence fixedSeq = iter.next();
			String seq = fixedSeq.getSequence();
			String name = fixedSeq.getId();
			// Check that all sequences are different
			if(seqs.contains(seq)) {
				throw new IllegalArgumentException("Sequence " + seq + " is used in multiple fixed sequence objects");
			}
			// Check that all names are different
			if(names.contains(name)) {
				throw new IllegalArgumentException("Name " + name + " is used for multiple fixed sequence objects");
			}
			seqs.add(seq);
			names.add(name);
			// Check that all lengths are equal
			if(fixedSeq.getLength() != length) {
				throw new IllegalArgumentException("All fixed sequences in switch must have same length.");
			}
		}
		fixedSequences = fixedSeqs;
	}
	
	/**
	 * Make a collection of two fixed sequences
	 * @param fixedSeqName1
	 * @param sequence1
	 * @param maxMismatches1
	 * @param fixedSeqName2
	 * @param sequence2
	 * @param maxMismatches2
	 * @return
	 */
	private static Collection<FixedSequence> makeFixedSeqCollection(String fixedSeqName1, String sequence1, int maxMismatches1, String fixedSeqName2, String sequence2, int maxMismatches2) {
		Collection<FixedSequence> rtrn = new ArrayList<FixedSequence>();
		rtrn.add(new FixedSequence(fixedSeqName1, sequence1, maxMismatches1));
		rtrn.add(new FixedSequence(fixedSeqName2, sequence2, maxMismatches2));
		return rtrn;
	}
	
	/**
	 * Construct with two fixed sequence options
	 * @param fixedSeqName1
	 * @param sequence1
	 * @param maxMismatches1
	 * @param fixedSeqName2
	 * @param sequence2
	 * @param maxMismatches2
	 */
	public Switch(String fixedSeqName1, String sequence1, int maxMismatches1, String fixedSeqName2, String sequence2, int maxMismatches2) {
		this(makeFixedSeqCollection(fixedSeqName1, sequence1, maxMismatches1, fixedSeqName2, sequence2, maxMismatches2));
	}
	
	@Override
	public int getLength() {
		return length;
	}

	@Override
	public boolean matchesFullString(String s) {
		for(FixedSequence fixedSeq : fixedSequences) {
			if(fixedSeq.matchesFullString(s)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public boolean matchesSubstringOf(String s, int startOnString) {
		for(FixedSequence fixedSeq : fixedSequences) {
			if(fixedSeq.matchesSubstringOf(s, startOnString)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public boolean isRepeatable() {
		return false;
	}

	@Override
	public String getStopSignalForRepeatable() {
		return null;
	}

	@Override
	public String matchedElementSequence(String s) {
		for(FixedSequence fixedSeq : fixedSequences) {
			if(fixedSeq.matchesFullString(s)) {
				return fixedSeq.getSequence();
			}
		}
		return null;
	}

	@Override
	public ReadSequenceElement matchedElement(String s) {
		for(FixedSequence fixedSeq : fixedSequences) {
			if(fixedSeq.matchesFullString(s)) {
				return fixedSeq;
			}
		}
		return null;
	}

	@Override
	public String elementName() {
		return "switch";
	}

	@Override
	public String getId() {
		String rtrn = "switch";
		for(FixedSequence fixedSeq : fixedSequences) {
			rtrn += "_" + fixedSeq.getId();
		}
		return rtrn;
	}

}
