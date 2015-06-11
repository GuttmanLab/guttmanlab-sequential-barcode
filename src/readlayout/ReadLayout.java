package readlayout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


import org.apache.log4j.Logger;

import readelement.ReadSequenceElement;

import com.sleepycat.persist.model.Persistent;

/**
 * A sequence of read elements that should be present in a read
 * Can have other stuff between them
 * @author prussell
 *
 */
@Persistent
public class ReadLayout {
	
	private ArrayList<ReadSequenceElement> elements;
	private int readLen;
	
	/*
	 * The length of the part of the read up to the end of the last matched element
	 * Includes positions before and between elements if allowed
	 * This is not a constant; it changes every time we match a new read sequence
	 */
	private int totalLengthMatchedEltSection;
	
	public static Logger logger = Logger.getLogger(ReadLayout.class.getName());
	
	/**
	 * For Berkeley DB only
	 * Do not use this constructor
	 */
	public ReadLayout() {}
	
	/**
	 * @param elementSequence Sequence of elements expected in each read
	 * @param readLength Read length
	 */
	public ReadLayout(ArrayList<ReadSequenceElement> elementSequence, int readLength) {
		int totalLen = 0;
		for(ReadSequenceElement elt : elementSequence) {
			totalLen += elt.getLength();
		}
		if(totalLen > readLength) {
			throw new IllegalArgumentException("Total length of read sequence elements (" + totalLen + ") must be at most declared read length (" + readLength + ").");
		}
		elements = elementSequence;
		readLen = readLength;
	}
	
	/**
	 * Get names of read elements expected in each read
	 * @return Ordered list of read element names
	 */
	public ArrayList<String> getElementNames() {
		ArrayList<String> rtrn = new ArrayList<String>();
		for(ReadSequenceElement elt : elements) {
			rtrn.add(elt.elementName());
		}
		return rtrn;
	}
	
	/**
	 * Get read elements expected in each read
	 * @return Ordered list of read elements
	 */
	public ArrayList<ReadSequenceElement> getElements() {
		return elements;
	}
	
	/**
	 * Get the length in the read of all the matched elements including positions before and between them
	 * i.e., the length of the read minus anything that comes after the last matched element
	 * @param readSequence Read sequence
	 * @return Total length of all matched elements and positions before and between them
	 * @throws IOException 
	 */
	public int matchedElementsLengthInRead(String readSequence) throws IOException {
		try {
			if(totalLengthMatchedEltSection == -1) {
				throw new IllegalArgumentException("Read does not match layout");
			}
			return totalLengthMatchedEltSection;
		} catch(NullPointerException e) {
			throw new IllegalStateException("Must call getMatchedElements first");
		}
	}
	
	/**
	 * Get a list of matched elements in the read sequence
	 * Only returns an object if the whole layout matches the read sequence
	 * Each item on list corresponds to one read sequence element in the layout
	 * List item is an ordered list of matched elements for that read element
	 * If whole layout does not match read sequence, returns null
	 * @param readSequence Read sequence to search for matches to this layout
	 * @return List of lists of matched elements from this layout that appear in the read
	 * @throws IOException 
	 */
	public List<List<ReadSequenceElement>> getMatchedElements(String readSequence) {
		
		List<List<ReadSequenceElement>> rtrn = new ArrayList<List<ReadSequenceElement>>();
		for(int i = 0; i < elements.size(); i++) {
			rtrn.add(new ArrayList<ReadSequenceElement>());
		}
		
		// Check that the read sequence has the read length required by this layout
		if(readSequence.length() != readLen) {
			logger.debug("WRONG_LENGTH\tRead layout " + toString() + " does not match read " + readSequence + " because lengths are different: " + readLen + ", " + readSequence.length());
			return null;
		}
		
		// Look for all the elements in order; can have other stuff between them
		Iterator<ReadSequenceElement> elementIter = elements.iterator();
		
		// For repeatable elements, save the first occurrences of their "next" element so can keep looking up until next element
		Map<ReadSequenceElement, Integer> stopSignalPos = new HashMap<ReadSequenceElement, Integer>();
		for(ReadSequenceElement elt : elements) {
			if(elt.isRepeatable()) {
				// First check if element has a stop signal
				ReadSequenceElement stopSignal = elt.getStopSignalForRepeatable();
				if(stopSignal == null) {
					stopSignalPos.put(elt, Integer.MAX_VALUE);
					continue;
				}
				int posNext = stopSignal.firstMatch(readSequence); // EFFICIENCY make firstMatch more efficient
				if(posNext != -1) {
					stopSignalPos.put(elt, Integer.valueOf(posNext));
					logger.debug("STOP_SIGNAL\t for element " + elt.getId() + " is at position " + posNext);
				}
			}
		}
		
		// Get the current element and look ahead to the next element
		// If current element is repeatable, will use next element to know when to stop looking for current element
		ReadSequenceElement currElt = elementIter.next();
		int currEltIndex = 0;
		ReadSequenceElement nextElt = null;
		if(elementIter.hasNext()) {
			nextElt = elementIter.next();
		}
		int currStart = 0;
		
		// Make sure all elements have been found at least once in the specified order
		boolean[] found = new boolean[elements.size()];
		while(currStart < readLen) {
			if(currElt == null) {
				totalLengthMatchedEltSection = currStart;
				return rtrn;
			}
			logger.debug("");
			logger.debug("CURRENT_START\t" + currStart);
			logger.debug("CURRENT_ELEMENT\t" + currElt.getId());
			logger.debug("NEXT_ELEMENT\t" + (nextElt == null ? null : nextElt.getId()));
			// If too far along in the read and have not found everything required, return null
			if(currStart + currElt.getLength() > readLen) {
				logger.debug("NO_MATCH_FOR_LAYOUT\tNo match for element " + currElt.getId() + " in read " + readSequence);
				// Change the length of matched elements section
				totalLengthMatchedEltSection = -1;
				return null;
			}
			// If current element is repeatable, look for next element at this position
			if(currElt.isRepeatable() && nextElt != null) {
				if(!(currStart + nextElt.getLength() > readLen)) {
					boolean lookNext = false;
					if(!stopSignalPos.containsKey(currElt)) {
						lookNext = true;
					} else if(stopSignalPos.get(currElt).intValue() == currStart) {
						lookNext = true;
					}
					if(lookNext) {
						logger.debug("LOOKING_FOR_NEXT_ELT\tLooking for " + nextElt.getId() + " at position " + currStart);
						if(nextElt != null && nextElt.matchesSubstringOf(readSequence, currStart)) {
							if(!found[currEltIndex]) {
								// Next element was found before any instance of current element
								logger.debug("FOUND_NEXT_BEFORE_CURRENT\tFound match for next element " + nextElt.getId() + " before any instance of " + currElt.getId());
								// Change the length of matched elements section
								totalLengthMatchedEltSection = -1;
								return null;
							}
							logger.debug("FOUND_NEXT_OK\tFound match for next element " + nextElt.getId() + " at start position " + currStart + " of read " + readSequence);
							found[elements.indexOf(nextElt)] = true;
							rtrn.get(elements.indexOf(nextElt)).add(nextElt.matchedElement(readSequence.substring(currStart, currStart + nextElt.getLength())));
							logger.debug("NUM_MATCHES\tThere are " + rtrn.get(elements.indexOf(nextElt)).size() + " matches for this element");
							currStart += nextElt.getLength();
							if(!elementIter.hasNext()) {
								// We have found a match for the last element; return
								logger.debug("MATCHED_LAYOUT\tFound match for entire read layout");
								// Change the length of matched elements section
								totalLengthMatchedEltSection = currStart;
								return rtrn;
							} 
							// Now look for the element after "nextElt"
							currElt = elementIter.next();
							currEltIndex++;
							logger.debug("NEW_CURR_ELT\t" + currElt.getId());
							if(elementIter.hasNext()) {
								nextElt = elementIter.next();
								logger.debug("NEW_NEXT_ELT\t" + nextElt.getId());
							} else {
								logger.debug("NEW_NEXT_ELT\tnull");
								nextElt = null;
							}
							continue;
						}
					}
					logger.debug("NOT_LOOKING_FOR_NEXT_ELT\tNot looking for " + nextElt.getId() + " at position " + currStart);
				}
			}
			// Look for current element
			if(currElt.matchesSubstringOf(readSequence, currStart)) {
				// Found an instance of current element
				logger.debug("MATCHED_CURRENT_ELEMENT\tFound match for element " + currElt.getId() + " at start position " + currStart + " of read " + readSequence);
				found[currEltIndex] = true;
				// Add to return data structure
				rtrn.get(currEltIndex).add(currElt.matchedElement(readSequence.substring(currStart, currStart + currElt.getLength())));
				logger.debug("NUM_MATCHES\tThere are " + rtrn.get(currEltIndex).size() + " matches for this element of length " + currElt.getLength() + ".");
				// Change current position to end of element
				currStart += currElt.getLength();
				// Now will look for the next element unless the current element is repeatable
				if(!currElt.isRepeatable()) {
					currElt = nextElt;
					currEltIndex++;
					nextElt = elementIter.hasNext() ? elementIter.next() : null;
					if(currElt != null) logger.debug("NEW_CURR_ELT\t" + currElt.getId());
					else logger.debug("NO_NEW_CURR_ELT");
					if(nextElt != null) logger.debug("NEW_NEXT_ELT\t" + nextElt.getId());
					else logger.debug("NO_NEW_NEXT_ELT");
				}
				continue;
			}
			logger.debug("No match for element " + currElt.getId() + " at start position " + currStart + " of read " + readSequence);
			currStart++;
		}
		// Change the length of matched elements section
		totalLengthMatchedEltSection = -1;
		return null;
	}
	
	public String toString() {
		Iterator<ReadSequenceElement> iter = elements.iterator();
		String rtrn = iter.next().elementName();
		while(iter.hasNext()) {
			rtrn += "_" + iter.next().elementName();
		}
		return rtrn;
	}
	
}
