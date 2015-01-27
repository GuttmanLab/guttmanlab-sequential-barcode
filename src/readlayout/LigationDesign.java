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
	SINGLE_DESIGN_BARCODE_IN_READ2_WITH_SWITCH,
		
	/**
	 * The design from late 2014
	 * All barcodes, DPM+DNA are in read2
	 */
	SINGLE_DESIGN_BARCODE_IN_READ2;
		
	public String toString() {
		switch(this) {
		case PAIRED_DESIGN_BARCODE_IN_READ2:
			return "paired_design_barcode_in_read2";
		case SINGLE_DESIGN_BARCODE_IN_READ2_WITH_SWITCH:
			return "single_design_barcode_in_read2_with_switch";
		case SINGLE_DESIGN_BARCODE_IN_READ2:
			return "single_design_barcode_in_read2";
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
		if(name.equals(SINGLE_DESIGN_BARCODE_IN_READ2_WITH_SWITCH.toString())) return SINGLE_DESIGN_BARCODE_IN_READ2_WITH_SWITCH;
		if(name.equals(SINGLE_DESIGN_BARCODE_IN_READ2.toString())) return SINGLE_DESIGN_BARCODE_IN_READ2;
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
