package readlayout;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import readelement.ReadSequenceElement;

public class ReadLayoutSequenceHash {
	
	public static Logger logger = Logger.getLogger(ReadLayoutSequenceHash.class.getName());
	
	private int maxMismatches;
	/*
	 * Overall key is number of mismatches. HashMap key is sequence. Hashmap value is index of represented element in array
	 */
	private Map<Integer, HashMap<String, Integer>> seqToElementIndex;
	private ReadSequenceElement[] elements;
	private static char[] alphabet = {'A', 'C', 'G', 'T'};
	
	public ReadLayoutSequenceHash(ReadLayout layout, int maxNumMismatches) {
		maxMismatches = maxNumMismatches;
		initialize(layout);
	}
	
	private void initialize(ReadLayout layout) {
		// Initialize the map of number of mismatches to imperfect sequence to element index
		seqToElementIndex = new HashMap<Integer, HashMap<String, Integer>>();
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
			String seq = elements[eltIndex].getSequence();
			logger.debug("");
			logger.debug("Getting representatives for element " + eltIndex + "\t" + elements[eltIndex].getId() + "\t" + seq);
			for(int numMismatch = 0; numMismatch <= maxMismatches; numMismatch++) {
				logger.debug(numMismatch + " mismatches:");
				Collection<String> mutatedSeqs = getRepresentatives(seq, numMismatch);
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
	 * Get the chars in the alphabet except this one
	 * @param c Any char. Throws exception if not in alphabet.
	 * @return Chars in the alphabet not equal to this one
	 */
	private static char[] otherChars(char c) {
		char[] rtrn = new char[alphabet.length - 1];
		boolean found = false;
		int curr = 0;
		int i = 0;
		while(i < alphabet.length) {
			if(c == alphabet[i]) {
				found = true;
				i++;
				continue;
			} else {
				rtrn[curr] = alphabet[i];
				curr++;
				i++;
			}
		}
		if(!found) {
			throw new IllegalArgumentException("Char " + c + " not in alphabet");
		}
		return rtrn;
	}
	
	/**
	 * Systematically replace each position with each other char in alphabet
	 * @param seq Sequence
	 * @return Set of sequences with hamming distance one from original sequence
	 */
	private static Collection<String> introduceOneMismatch(String seq) {
		char[] charSeq = seq.toUpperCase().toCharArray();
		Collection<String> rtrn = new HashSet<String>();
		for(int i = 0; i < charSeq.length; i++) {
			char realChar = charSeq[i];
			char[] otherChars = otherChars(realChar);
			for(int j = 0; j < otherChars.length; j++) {
				charSeq[i] = otherChars[j];
				rtrn.add(new String(charSeq));
			}
			charSeq[i] = realChar;
		}
		return rtrn;
	}
	
	/**
	 * Get a set containing all possible strings with hamming distance one from a string in the input set
	 * No strings in return set will have hamming distance > 1 from any string in the input set
	 * Some strings in return set may have hamming distance 0 from a string in input set
	 * @param seqs Input strings
	 * @return Set of all strings of hamming distance 1 from a string in input set, plus maybe some with hamming distance 0
	 */
	private static Collection<String> introduceAllPossibleSingleMismatches(Collection<String> seqs) {
		Collection<String> rtrn = new HashSet<String>();
		for(String seq : seqs) {
			rtrn.addAll(introduceOneMismatch(seq));
		}
		return rtrn;
	}
	
	/**
	 * Get all possible versions of the sequence with this many mismatches, plus maybe some with fewer mismatches
	 * @param seq Sequence
	 * @param mismatches Number of mismatches
	 * @return A set containing all versions of the sequence with the requested number of mismatches, plus maybe some with fewer mismatches
	 */
	private static Collection<String> getRepresentatives(String seq, int mismatches) {
		if(mismatches == 0) {
			Collection<String> rtrn = new HashSet<String>();
			rtrn.add(seq);
			return rtrn;
		}
		Collection<String> prev = new HashSet<String>();
		prev.add(seq);
		int mismatchesDone = 0;
		while(true) {
			Collection<String> mutated = introduceAllPossibleSingleMismatches(prev);
			mismatchesDone++;
			if(mismatchesDone == mismatches) {
				return mutated;
			}
			prev.clear();
			prev.addAll(mutated);
		}
	}
		
	public ReadSequenceElement bestMatch(String sequence) {
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
