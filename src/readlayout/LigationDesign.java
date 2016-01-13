package readlayout;

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
	PAIRED_DESIGN_BARCODE_IN_READ2,
	
	/**
	 * The design from late 2014
	 * All barcodes, RPM+RNA, DPM+DNA are in read2
	 */
	SINGLE_DESIGN_WITH_SWITCH,
		
	/**
	 * The design from late 2014
	 * All barcodes, DPM+DNA are in read2
	 */
	SINGLE_DESIGN,
	
	/**
	 * The design from March 2015
	 * All barcodes are followed by zero or more adapters, which are followed by the read sequence
	 * No DPM
	 */
	SINGLE_DESIGN_MARCH_2015,
		
	/**
	 * The design from May 2015
	 * Read layout:
	 * An extra barcode appears at beginning of read
	 * Next, a series of barcodes that fall into equivalence classes
	 * 		Classes of 4 barcodes each are considered a single barcode
	 * Finally, another barcode
	 * No fixed sequence separates barcodes from DNA
	 */
	SINGLE_DESIGN_MAY_2015,
	
	/**
	 * The design from 7/13/15
	 * Even/odd barcodes in read 2
	 * Read 1 contains a single barcode at the beginning and then DNA sequence
	 */
	PAIRED_DESIGN_JULY_2015,
	
	/**
	 * Design from January 2016
	 * Read 1 contains RPM or DPM at known position
	 * Assume data have been divided into RNA/DNA and RPM/DPM removed before running this
	 * Barcodes are in read2
	 */
	PAIRED_DESIGN_JANUARY_2016;
	
		
	public String toString() {
		switch(this) {
		case PAIRED_DESIGN_BARCODE_IN_READ2:
			return "paired_design_barcode_in_read2";
		case SINGLE_DESIGN_WITH_SWITCH:
			return "single_design_with_switch";
		case SINGLE_DESIGN:
			return "single_design";
		case SINGLE_DESIGN_MARCH_2015:
			return "single_design_march_2015";
		case SINGLE_DESIGN_MAY_2015:
			return "single_design_may_2015";
		case PAIRED_DESIGN_JULY_2015:
			return "paired_design_july_2015";
		case PAIRED_DESIGN_JANUARY_2016:
			return "paired_design_january_2015";
		default:
			throw new UnsupportedOperationException("Not implemented");
		}
	}
	
	/**
	 * Create from name
	 * @param name Design name
	 * @return The design
	 */
	public static LigationDesign fromString(String name) {
		if(name.equals(PAIRED_DESIGN_BARCODE_IN_READ2.toString())) return PAIRED_DESIGN_BARCODE_IN_READ2;
		if(name.equals(SINGLE_DESIGN_WITH_SWITCH.toString())) return SINGLE_DESIGN_WITH_SWITCH;
		if(name.equals(SINGLE_DESIGN.toString())) return SINGLE_DESIGN;
		if(name.equals(SINGLE_DESIGN_MARCH_2015.toString())) return SINGLE_DESIGN_MARCH_2015;
		if(name.equals(SINGLE_DESIGN_MAY_2015.toString())) return SINGLE_DESIGN_MAY_2015;
		if(name.equals(PAIRED_DESIGN_JULY_2015.toString())) return PAIRED_DESIGN_JULY_2015;
		if(name.equals(PAIRED_DESIGN_JANUARY_2016.toString())) return PAIRED_DESIGN_JANUARY_2016;
		throw new IllegalArgumentException("Name " + name + " not recognized. Options: " + getNamesAsCommaSeparatedList());
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
