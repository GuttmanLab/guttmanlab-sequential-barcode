package readlayout;

import java.util.ArrayList;
import java.util.Iterator;

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
	 * Get the read length
	 * @return The read length
	 */
	public int getReadLength() {
		return readLen;
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
	
	
	public String toString() {
		Iterator<ReadSequenceElement> iter = elements.iterator();
		String rtrn = iter.next().elementName();
		while(iter.hasNext()) {
			rtrn += "_" + iter.next().elementName();
		}
		return rtrn;
	}
	
		
	
}
