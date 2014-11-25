package sequentialbarcode.readlayout;

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
	 * All barcodes, RPM, and RNA sequence are in read2
	 */
	SINGLE_DESIGN_RNA_BARCODE_IN_READ2,
	
	/**
	 * The design from late 2014
	 * All barcodes, DPM, and DNA sequence are in read2
	 */
	SINGLE_DESIGN_DNA_BARCODE_IN_READ2;
	
}
