package util;

import java.util.Arrays;
import java.util.Collection;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class CustomSamTag {
	
	private CustomSamTag() {};
	
	public static final String BARCODE_SEQUENCE = "XB";
	public static final String TRANSCRIPT_OVERLAPPERS = "XT";
	public static final String HAS_ALL_BARCODES = "XA";
	public static final String RRNA_ALIGNED = "XR";
	public static final String CONTAINED_IN_ANNOTATION = "XF";
	
	/**
	 * Tag indicating whether the read originated from RNA or DNA
	 */
	public static final String RNA_DNA = "XX";
	
	/**
	 * Value of the RNA/DNA tag indicating RNA
	 */
	public static final String RNA_DNA_VAL_RNA = "RNA";
	
	/**
	 * Value of the RNA/DNA tag indicating DNA
	 */
	public static final String RNA_DNA_VAL_DNA = "DNA";
	
	/**
	 * Get the transcript IDs listed in a transcript overlapper tag value
	 * @param transcriptOverlapperTagValue Tag value to parse
	 * @return Collection of transcript IDs
	 */
	public static Collection<String> transcriptIDs(String transcriptOverlapperTagValue) {
		return Arrays.asList(transcriptOverlapperTagValue.split("\\["))
				.stream()
				.map(str -> str.replaceAll("]", ""))
				.collect(Collectors.toCollection(TreeSet::new));
	}
	
	
	
}
