package matcher;

import java.io.IOException;
import java.util.List;

import readelement.ReadSequenceElement;
import readlayout.ReadLayout;

/**
 * 
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
	 * Get the length in the read of all the matched elements including positions before and between them
	 * i.e., the length of the read minus anything that comes after the last matched element
	 * @return Total length of all matched elements and positions before and between them
	 * @throws IOException 
	 */
	public int matchedElementsLengthInRead();
	
}
