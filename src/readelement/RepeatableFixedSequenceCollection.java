package readelement;

import guttmanlab.core.sequence.FastaFileIOImpl;
import guttmanlab.core.sequence.Sequence;

import java.util.ArrayList;
import java.util.Collection;

/**
 * A collection of possibilities of fixed sequences
 * Several instances can occur in a row in the read
 * @author prussell
 *
 */
public class RepeatableFixedSequenceCollection extends Switch {
		
	/**
	 * @param fixedSeqs The fixed sequence options for the read element
	 */
	public RepeatableFixedSequenceCollection(Collection<FixedSequence> fixedSeqs) {
		super(fixedSeqs);
	}
	
	/**
	 * @param seqFasta Fasta file of fixed sequences
	 * @param maxMismatches Max number of mismatches to apply to all fixed sequences
	 */
	public RepeatableFixedSequenceCollection(String seqFasta, int maxMismatches) {
		this(makeFixedSeqCollection(seqFasta, maxMismatches));
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
		return true;
	}

	@Override
	public String getStopSignalForRepeatable() {
		return null;
	}

	@Override
	public String elementName() {
		return "repeatable_fixed_sequence_collection";
	}

	@Override
	public String getId() {
		String rtrn = "repeatable_fixed_sequence_collection";
		for(FixedSequence fixedSeq : fixedSequences) {
			rtrn += "_" + fixedSeq.getId();
		}
		return rtrn;
	}

}
