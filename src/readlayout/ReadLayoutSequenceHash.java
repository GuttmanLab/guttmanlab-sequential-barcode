package readlayout;

import guttmanlab.core.util.MismatchGenerator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import readelement.ReadSequenceElement;

/**
 * Hash-based cache of imperfect sequences mapped to the sequence they represent
 * Stores all possible imperfect matches for an entire read layout
 * @author prussell
 *
 */
public final class ReadLayoutSequenceHash {
	
	public static final Logger logger = Logger.getLogger(ReadLayoutSequenceHash.class.getName());
	
	/*
	 * Overall key is number of mismatches. HashMap key is sequence. Hashmap value is index of represented element in array
	 */
	private Map<Integer, HashMap<String, Integer>> seqToElementIndex;
	private ReadSequenceElement[] elements;
	
	/**
	 * @param layout Read layout
	 */
	public ReadLayoutSequenceHash(ReadLayout layout) {
		initialize(layout);
	}
	
	private void initialize(ReadLayout layout) {
		// Initialize the map of number of mismatches to imperfect sequence to element index
		seqToElementIndex = new HashMap<Integer, HashMap<String, Integer>>();
		int maxMismatches = 0;
		for(ReadSequenceElement elt : layout.getElements()) {
			if(elt.maxLevenshteinDist() > maxMismatches) maxMismatches = elt.maxLevenshteinDist();
		}
		for(int i = 0; i <= maxMismatches; i++) {
			seqToElementIndex.put(Integer.valueOf(i), new HashMap<String, Integer>());
		}
		
		// Determine all possible ReadSequenceElements that can be identified
		Set<ReadSequenceElement> possibleElts = new HashSet<ReadSequenceElement>();
		for(ReadSequenceElement element : layout.getElements()) {
			logger.debug("Adding possible element " + element.getId());
			possibleElts.addAll(element.sequenceToElement().values());
		}
		logger.debug("");
		// Put them in a list
		List<ReadSequenceElement> possibleEltsList = new ArrayList<ReadSequenceElement>();
		possibleEltsList.addAll(possibleElts);
		// Put them in the array
		elements = new ReadSequenceElement[possibleEltsList.size()];
		for(int i = 0; i < elements.length; i++) {
			elements[i] = possibleEltsList.get(i);
			logger.debug("elements[" + i + "]\t" + elements[i].getId() + "\t" + elements[i].getSequence());
		}
		
		// Store mapping from number of mismatches to mutated sequence to index of element
		logger.debug("");
		for(int eltIndex = 0; eltIndex < elements.length; eltIndex++) {
			ReadSequenceElement elt = elements[eltIndex];
			String seq = elt.getSequence();
			logger.debug("");
			logger.debug("Getting representatives for element " + eltIndex + "\t" + elements[eltIndex].getId() + "\t" + seq);
			for(int numMismatch = 0; numMismatch <= elt.maxLevenshteinDist(); numMismatch++) {
				logger.debug(numMismatch + " mismatches:");
				Collection<String> mutatedSeqs = MismatchGenerator.getRepresentatives(seq, numMismatch);
				for(String mutated : mutatedSeqs) {
					if(seqToElementIndex.get(Integer.valueOf(numMismatch)).containsKey(mutated)) {
						String elt1 = elements[seqToElementIndex.get(Integer.valueOf(numMismatch)).get(mutated).intValue()].getId();
						String elt2 = elements[eltIndex].getId();
						throw new IllegalStateException("Mutated sequence " + mutated + " matches multiple elements ("
								+ elt1 + ", " + elt2 + ") with " + numMismatch + " mismatches");
					}
					seqToElementIndex.get(Integer.valueOf(numMismatch)).put(mutated, Integer.valueOf(eltIndex));
					logger.debug(mutated + "\t" + seqToElementIndex.get(Integer.valueOf(numMismatch)).get(mutated).toString());
				}
			}
		}
	}
		
	
	/**
	 * Get best element match for a sequence
	 * @param sequence Actual observed sequence
	 * @param maxMismatches Max mismatches
	 * @return Cached element match with fewest mismatches
	 */
	public ReadSequenceElement bestMatch(String sequence, int maxMismatches) {
		for(int i = 0; i <= maxMismatches; i++) {
			Integer index = seqToElementIndex.get(Integer.valueOf(i)).get(sequence);
			if(index != null) {
				logger.debug("BEST_MATCH\tfor sequence " + sequence + " is " + elements[index.intValue()].getId() + "\t" + elements[index.intValue()].getSequence() + " (" + i + " mismatches)");
				return elements[index.intValue()];
			}
		}
		return null;
	}
	
}
