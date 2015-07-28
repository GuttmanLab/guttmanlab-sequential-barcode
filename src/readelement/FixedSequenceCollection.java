package readelement;

import guttmanlab.core.sequence.FastaFileIOImpl;
import guttmanlab.core.sequence.Sequence;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

/**
 * A collection of possibilities of fixed sequences
 * Sequences can have different lengths
 * @author prussell
 *
 */
public class FixedSequenceCollection extends AbstractReadSequenceElement {
	
	protected Collection<FixedSequence> fixedSequences;
	private int minLength;
	private int maxLength;
	private int maxLevDist;
	private boolean repeatable;
	
	/**
	 * @param fixedSeqs The fixed sequence options for the read element
	 * @param isRepeatable Whether multiple instances can occur in a row in the read
	 */
	public FixedSequenceCollection(Collection<FixedSequence> fixedSeqs, boolean isRepeatable) {
		minLength = Integer.MAX_VALUE;
		maxLength = 0;
		repeatable = isRepeatable;
		Collection<String> seqs = new HashSet<String>();
		Collection<String> names = new HashSet<String>();
		Iterator<FixedSequence> iter = fixedSeqs.iterator();
		FixedSequence first = iter.next();
		seqs.add(first.getSequence());
		names.add(first.getId());
		maxLevDist = fixedSeqs.iterator().next().maxLevenshteinDist();
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
			if(fixedSeq.maxLevenshteinDist() != maxLevDist) {
				throw new IllegalArgumentException("All fixed sequences in set must have same max Levenshtein distance");
			}
			// Record min and max lengths
			if(fixedSeq.getLength() > maxLength) {
				maxLength = fixedSeq.getLength();
			}
			if(fixedSeq.getLength() < minLength) {
				minLength = fixedSeq.getLength();
			}
			seqs.add(seq);
			names.add(name);
		}
		fixedSequences = fixedSeqs;
	}
	
	/**
	 * @param seqFasta Fasta file of fixed sequences
	 * @param maxMismatches Max number of mismatches to apply to all fixed sequences
	 * @param isRepeatable Whether multiple instances can occur in a row in the read
	 */
	public FixedSequenceCollection(String seqFasta, int maxMismatches, boolean isRepeatable) {
		this(makeFixedSeqCollection(seqFasta, maxMismatches), isRepeatable);
	}
	
	private static Collection<FixedSequence> makeFixedSeqCollection(String seqFasta, int maxMismatches) {
		FastaFileIOImpl fio = new FastaFileIOImpl();
		Collection<Sequence> seqs = fio.readFromFile(seqFasta);
		Collection<FixedSequence> rtrn = new ArrayList<FixedSequence>();
		for(Sequence seq : seqs) {
			rtrn.add(new FixedSequence(seq.getName(), seq.getSequenceBases().toUpperCase(), maxMismatches));
		}
		return rtrn;
	}
	
	@Override
	public boolean isRepeatable() {
		return repeatable;
	}

	@Override
	public ReadSequenceElement getStopSignalForRepeatable() {
		return null;
	}

	@Override
	public String elementName() {
		return "fixed_sequence_collection";
	}

	@Override
	public String getId() {
		String rtrn = "fixed_sequence_collection";
		for(FixedSequence fixedSeq : fixedSequences) {
			rtrn += "_" + fixedSeq.getId();
		}
		return rtrn;
	}

	@Override
	public int getLength() {
		if(minLength == maxLength) {
			return minLength;
		}
		throw new IllegalStateException("NA");
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
	public boolean matchesSubstringNoGaps(String s, int startOnString) {
		for(FixedSequence fixedSeq : fixedSequences) {
			if(fixedSeq.matchesSubstringNoGaps(s, startOnString)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public MatchedElement matchedElement(String s) {
		throw new UnsupportedOperationException("NA"); 
		// Don't want to do it this way anymore
//		for(FixedSequence fixedSeq : fixedSequences) {
//			if(fixedSeq.matchesSubstringNoGaps(s, 0)) {
//				return new MatchedElement(fixedSeq, fixedSeq.getLength());
//			}
//		}		
//		for(FixedSequence fixedSeq : fixedSequences) {
//			MatchedElement matchedElt = fixedSeq.matchedElement(s);
//			if(matchedElt != null) {
//				return matchedElt;
//			}
//		}
//		return null;
	}

	@Override
	public String getSequence() {
		return null;
	}

	@Override
	public Map<String, ReadSequenceElement> sequenceToElement() {
		Map<String, ReadSequenceElement> rtrn = new HashMap<String, ReadSequenceElement>();
		for(FixedSequence seq : fixedSequences) {
			rtrn.putAll(seq.sequenceToElement());
		}
		return rtrn;
	}

	@Override
	public int minMatch() {
		throw new UnsupportedOperationException("NA");
	}

	@Override
	public int maxLevenshteinDist() {
		return maxLevDist;
	}

}
