package readelement;

import java.util.Map;

/**
 * An element that can be found in a read sequence
 * @author prussell
 *
 */
public interface ReadSequenceElement {
	
	/**
	 * Get element length
	 * @return Element length
	 */
	public int getLength();
	
	/**
	 * Check whether this element matches a string
	 * @param s The string
	 * @return True if this element matches the string
	 */
	public boolean matchesFullString(String s);
	
	/**
	 * Check whether this element matches a substring of a string, where both element and substring have same length (no gaps)
	 * @param s The string
	 * @param startOnString Start position of substring within string
	 * @return True if this element matches the substring starting at the specified position with no gaps
	 */
	public boolean matchesSubstringNoGaps(String s, int startOnString);
	
	/**
	 * Get the position of the first match of this element within the string
	 * @param s The string
	 * @return Zero based position of first match, or -1 if no match
	 */
	public int firstMatch(String s);
	
	/**
	 * Whether instances of the element can appear an arbitrary number of times in tandem within a read
	 * @return True iff element is repeatable
	 */
	public boolean isRepeatable();
	
	/**
	 * For repeatable read elements, a string whose presence signals the repeat is over
	 * @return A sequence that comes after the repeatable element, or null if not repeatable
	 */
	public ReadSequenceElement getStopSignalForRepeatable();
	
	/**
	 * Get the read element matching the sequence
	 * @param s Read subsequence to search
	 * @return The element that matches the sequence or null if it doesn't match
	 */
	public MatchedElement matchedElement(String s);
	
	/**
	 * Get the read element matching the sequence
	 * @param s Read subsequence to search
	 * @param startPosOnString Start position on string
	 * @return The element that matches the substring starting at given position, or null if it doesn't match
	 */
	public MatchedElement matchedElement(String s, int startPosOnString);
	
	/**
	 * Get the general name of the element
	 * @return General element name
	 */
	public String elementName();
	
	/**
	 * Get the specific ID of this instance
	 * @return Element ID
	 */
	public String getId();
	
	/**
	 * Get the sequence of this element
	 * @return The sequence of this element, or null if not applicable (e.g. if a set of possible sequences)
	 */
	public String getSequence();
	
	/**
	 * Identify elemental sequences with the sequence elements they represent
	 * For example, in a BarcodeSet, sequences of single barcodes represent the single barcode,
	 * while in a BarcodeEquivalenceClass, sequences of single barcodes represent the equivalence class
	 * @return
	 */
	public Map<String, ReadSequenceElement> sequenceToElement();
	
	/**
	 * Get the minimum number of matched positions to call a match in a read
	 * @return Minimum number of matches in local alignment to read
	 */
	public int minMatch();
	
	/**
	 * Get the maximum Levenshtein distance to call a match to a read
	 * @return Maximum Levenshtein used to identify matches
	 */
	public int maxLevenshteinDist();
	
}
