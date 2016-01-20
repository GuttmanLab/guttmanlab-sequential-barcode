package readlayout;

import java.util.HashMap;
import java.util.Map;

/**
 * A design for the layout of the reads
 * @author prussell
 *
 */
public enum LigationDesign {
	
	/**
	 * The design from early 2014
	 * No distinction between RNA and DNA
	 * Barcode sequence is in read2
	 * Nucleic acid sequence is in read1
	 */
	PAIRED_DESIGN_BARCODE_IN_READ2("paired_design_barcode_in_read2"),
	
	/**
	 * The design from late 2014
	 * All barcodes, RPM+RNA, DPM+DNA are in read2
	 */
	SINGLE_DESIGN_WITH_SWITCH("single_design_with_switch"),
		
	/**
	 * The design from late 2014
	 * All barcodes, DPM+DNA are in read2
	 */
	SINGLE_DESIGN("single_design"),
	
	/**
	 * The design from March 2015
	 * All barcodes are followed by zero or more adapters, which are followed by the read sequence
	 * No DPM
	 */
	SINGLE_DESIGN_MARCH_2015("single_design_march_2015"),
		
	/**
	 * The design from May 2015
	 * Read layout:
	 * An extra barcode appears at beginning of read
	 * Next, a series of barcodes that fall into equivalence classes
	 * 		Classes of 4 barcodes each are considered a single barcode
	 * Finally, another barcode
	 * No fixed sequence separates barcodes from DNA
	 */
	SINGLE_DESIGN_MAY_2015("single_design_may_2015"),
	
	/**
	 * The design from 7/13/15
	 * Even/odd barcodes in read 2
	 * Read 1 contains a single barcode at the beginning and then DNA sequence
	 */
	PAIRED_DESIGN_JULY_2015("paired_design_july_2015"),
	
	/**
	 * Design from January 2016
	 * Read 1 contains RPM or DPM at known position
	 * Assume data have been divided into RNA/DNA and RPM/DPM removed before running this
	 * Barcodes are in read2
	 */
	PAIRED_DESIGN_JANUARY_2016("paired_design_january_2015");
	
	private LigationDesign(String name) {
		this.name = name;
	}
	
	private String name;
	private static Map<String, LigationDesign> fromName;
	
	// Save mapping of name to object
	static {
		fromName = new HashMap<String, LigationDesign>();
		for(LigationDesign ld : values()) {
			fromName.put(ld.toString(), ld);
		}
	}
	
	public String toString() {
		return name;
	}
	
	/**
	 * Create from name
	 * @param name Design name
	 * @return The design
	 */
	public static LigationDesign fromString(String name) {
		return fromName.get(name);
	}
	
	/**
	 * @return Comma separated list of names
	 */
	public static String getNamesAsCommaSeparatedList() {
		LigationDesign[] values = LigationDesign.values();
		String rtrn = values[0].toString();
		for(int i = 1; i < values.length; i++) {
			rtrn += ", " + values[i].toString();
		}
		return rtrn;
	}
	
}
