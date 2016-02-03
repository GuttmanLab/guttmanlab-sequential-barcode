package sequentialbarcode;


import guttmanlab.core.annotation.Annotation;

import org.apache.log4j.Logger;

import readlayout.ReadLayout;

/**
 * Barcoded fragment that originates from DNA
 * @author prussell
 *
 */
public final class BarcodedDNAFragment extends BasicBarcodedFragment {

	public static Logger logger = Logger.getLogger(BarcodedDNAFragment.class.getName());

	/**
	 * @param fragmentId Fragment ID
	 * @param read1seq Read1 sequence
	 * @param read2seq Read2 sequence
	 * @param barcodeSignature Barcodes for the fragment
	 */
	public BarcodedDNAFragment(String fragmentId, String read1seq, String read2seq, BarcodeSequence barcodeSignature) {
		super(fragmentId, read1seq, read2seq, barcodeSignature);
	}

	/**
	 * @param fragmentId Fragment ID
	 * @param read1seq Read1 sequence
	 * @param read2seq Read2 sequence
	 * @param barcodeSignature Barcodes for the fragment
	 * @param mappedLocation Mapped location of the fragment
	 */
	public BarcodedDNAFragment(String fragmentId, String read1seq, String read2seq, BarcodeSequence barcodeSignature, Annotation mappedLocation) {
		super(fragmentId, read1seq, read2seq, barcodeSignature, mappedLocation);
	}

	/**
	 * @param fragmentId Fragment ID
	 * @param read1seq Read1 sequence
	 * @param read2seq Read2 sequence
	 * @param layoutRead1 Read1 layout or null if not specified
	 * @param layoutRead2 Read2 layout or null if not specified
	 */
	public BarcodedDNAFragment(String fragmentId, String read1seq, String read2seq, ReadLayout layoutRead1, ReadLayout layoutRead2) {
		super(fragmentId, read1seq, read2seq, layoutRead1, layoutRead2);
	}

	
}
