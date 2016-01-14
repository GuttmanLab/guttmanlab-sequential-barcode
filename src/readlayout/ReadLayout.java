package readlayout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
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
	private Map<ReadSequenceElement, Map<String, ReadSequenceElement>> elementsByRepresentative;
	
	public static final Logger logger = Logger.getLogger(ReadLayout.class.getName());
	
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
		initializeElementsByRepresentative();
	}
	
	private void initializeElementsByRepresentative() {
		elementsByRepresentative = new HashMap<ReadSequenceElement, Map<String, ReadSequenceElement>>();
		for(ReadSequenceElement element : elements) {
			elementsByRepresentative.put(element, element.sequenceToElement());
		}
	}
	
	/**
	 * Get read element (part of a parent element) represented by a particular sequence that is one possible representative of a parent element
	 * For example, if parent element is a barcode equivalence class, any barcode in the equivalence class represents the whole equivalence class
	 * @param parentElement Parent element
	 * @param representative Sequence of representative element
	 * @return The sub-element of the parent element represented by the representative
	 */
	public final ReadSequenceElement getElementRepresentedBy(ReadSequenceElement parentElement, String representative) {
		return elementsByRepresentative.get(parentElement).get(representative);
	}
	
	/**
	 * Get map of representative sequences to sub-elements of a parent element that they represent
	 * @param parentElement Parent sequence element
	 * @return For the parent element, map of all representative sequences and the sub-element they represent
	 */
	public final Map<String, ReadSequenceElement> getSubElementsByRepresentative(ReadSequenceElement parentElement) {
		return elementsByRepresentative.get(parentElement);
	}
	
	/**
	 * Get the read length
	 * @return The read length
	 */
	public final int getReadLength() {
		return readLen;
	}
	
	/**
	 * Get names of read elements expected in each read
	 * @return Ordered list of read element names
	 */
	public final ArrayList<String> getElementNames() {
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
	public final ArrayList<ReadSequenceElement> getElements() {
		return elements;
	}
	
	
	public final String toString() {
		Iterator<ReadSequenceElement> iter = elements.iterator();
		String rtrn = iter.next().elementName();
		while(iter.hasNext()) {
			rtrn += "_" + iter.next().elementName();
		}
		return rtrn;
	}
	
		
	
}
