package matcher;

import java.io.IOException;
import java.util.List;

import readelement.ReadSequenceElement;
import readlayout.ReadLayout;

/**
 * Match a read layout to an actual read sequence and get the matched elements in order
 * @author prussell
 *
 */
public interface ElementMatcher {

	/**
	 * Get the list of matched elements in the read sequence
	 * Each item on list corresponds to one read sequence element in the layout
	 * List item is an ordered list of matched elements for that read element
	 * If whole layout does not match read sequence, returns null
	 * @return List of matched elements or null if whole layout doesn't match
	 * @throws IOException 
	 */
	public List<List<ReadSequenceElement>> getMatchedElements();
	
	/**
	 * Get the read layout
	 * @return The read layout
	 */
	public ReadLayout getReadLayout();
	
	/**
	 * Get the original sequence (e.g. the whole read)
	 * @return Full original sequence
	 */
	public String getOriginalSequence();
	
	/**
	 * Get the read sequence element that matches a read at a particular start position
	 * For example, if we are looking for a barcode set, and one barcode in the set matches
	 * the read at the position, this method should return the one barcode that matches.
	 * Return null if no match
	 * @param toMatch The read sequence element to match to the read
	 * @param startPosOnRead Start position of potential match
	 * @return The matching element from the element to be matched, or null if no match starting at this position
	 */
	public MatchedElement getMatchedElement(ReadSequenceElement toMatch, int startPosOnRead);
	
	/**
	 * Get the length in the read of all the matched elements including positions before and between them
	 * i.e., the length of the read minus anything that comes after the last matched element
	 * @return Total length of all matched elements and positions before and between them
	 * @throws IOException 
	 */
	public int matchedElementsLengthInRead();
	
	/**
	 * Optionally store element matches up front
	 */
	public void cacheMatches();
	
}
