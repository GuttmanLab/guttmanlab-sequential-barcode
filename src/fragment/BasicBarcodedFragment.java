package fragment;


import guttmanlab.core.annotation.Annotation;
import guttmanlab.core.annotation.SingleInterval;
import guttmanlab.core.util.StringParser;
import htsjdk.samtools.SAMRecord;

import java.util.List;

import org.apache.log4j.Logger;

import contact.BarcodeSequence;
import readelement.FragmentBarcode;
import readelement.BarcodeEquivalenceClass;
import readelement.BarcodeEquivalenceClassSet;
import readelement.BarcodeSet;
import readelement.ReadSequenceElement;
import readlayout.ReadLayout;
import matcher.BitapMatcher;

/**
 * A basic implementation of a barcoded fragment
 * @author prussell
 *
 */
public class BasicBarcodedFragment implements BarcodedFragment {
	protected String infoString;
	protected String id;
	protected String read1sequence;
	protected String read2sequence;
	protected String unpairedSequence;
	protected Annotation location;
	protected ReadLayout read1layout;
	protected ReadLayout read2layout;
	protected BarcodeSequence barcodes;
	public static Logger logger = Logger.getLogger(BasicBarcodedFragment.class.getName());
	
	/**
	 * @param fragmentId Fragment ID
	 * @param barcodeSignature Barcodes for the fragment
	 * @param mappedChr Mapped chromosome for the fragment
	 * @param mappedStart Mapped start
	 * @param mappedEnd Mapped end
	 */
	public BasicBarcodedFragment(String fragmentId, BarcodeSequence barcodeSignature, String mappedChr, int mappedStart, int mappedEnd) {
		this(fragmentId, barcodeSignature, new SingleInterval(mappedChr, mappedStart, mappedEnd));
	}
	
	/**
	 * @param fragmentId Fragment ID
	 * @param barcodeSignature Barcodes for the fragment
	 * @param mappedLocation Mapped location of the fragment
	 */
	public BasicBarcodedFragment(String fragmentId, BarcodeSequence barcodeSignature, Annotation mappedLocation) {
		id = StringParser.firstField(fragmentId);
		setBarcodes(barcodeSignature);
		location = mappedLocation;
		infoString = getInfoString(id, location.getReferenceName(), location.getReferenceStartPosition(), location.getReferenceEndPosition());
	}
	
	/**
	 * Get number of barcodes
	 * @return Number of barcodes
	 */
	public int getNumBarcodes() {
		return barcodes.getNumBarcodes();
	}
	
	private void setBarcodes(BarcodeSequence bs) {
		barcodes = bs;
	}
	
	/**
	 * Get the info string that is used as the unique primary key for a mapping
	 * @param readID Read ID
	 * @param chr Mapped chromosome
	 * @param start Mapped start
	 * @param end Mapped end
	 * @return Info string
	 */
	public static String getInfoString(String readID, String chr, int start, int end) {
		return readID + ":" + chr + ":" + start + "-" + end;
	}
	
	/**
	 * Get fragment ID from sam record
	 * @param samRecord Sam record
	 * @return Fragment ID
	 */
	public static String getIdFromSamRecord(SAMRecord samRecord) {
		return StringParser.firstField(samRecord.getReadName());
	}
	
	
	/**
	 * @param fragmentId Fragment ID
	 * @param read1seq Read1 sequence
	 * @param read2seq Read2 sequence
	 * @param barcodeSignature Barcodes for the fragment
	 */
	public BasicBarcodedFragment(String fragmentId, String read1seq, String read2seq, BarcodeSequence barcodeSignature) {
		this(fragmentId, read1seq, read2seq, barcodeSignature, null);
	}

	/**
	 * @param fragmentId Fragment ID
	 * @param read1seq Read1 sequence
	 * @param read2seq Read2 sequence
	 * @param barcodeSignature Barcodes for the fragment
	 * @param mappedLocation Mapped location of the fragment
	 */
	public BasicBarcodedFragment(String fragmentId, String read1seq, String read2seq, BarcodeSequence barcodeSignature, Annotation mappedLocation) {
		id = StringParser.firstField(fragmentId);
		read1sequence = read1seq;
		read2sequence = read2seq;
		setBarcodes(barcodeSignature);
		location = mappedLocation;
		infoString = getInfoString(id, location.getReferenceName(), location.getReferenceStartPosition(), location.getReferenceEndPosition());
	}

	/**
	 * @param fragmentId Fragment ID
	 * @param read1seq Read1 sequence
	 * @param read2seq Read2 sequence
	 * @param layoutRead1 Read1 layout or null if not specified
	 * @param layoutRead2 Read2 layout or null if not specified
	 */
	public BasicBarcodedFragment(String fragmentId, String read1seq, String read2seq, ReadLayout layoutRead1, ReadLayout layoutRead2) {
		id = StringParser.firstField(fragmentId);
		read1sequence = read1seq;
		read2sequence = read2seq;
		read1layout = layoutRead1;
		read2layout = layoutRead2;
	}
	
	public final BarcodeSequence getBarcodes() {
		return getBarcodes(null, null);
	}
	
	/**
	 * Get barcodes where we have already matched elements to the read(s)
	 * @param matchedEltsRead1 Matched elements for read 1 or null if identifying here for the first time or there is no read 1
	 * @param matchedEltsRead2 Matched elements for read 2 or null if identifying here for the first time or there is no read 2
	 */
	public final BarcodeSequence getBarcodes(List<List<ReadSequenceElement>> matchedEltsRead1, List<List<ReadSequenceElement>> matchedEltsRead2) {
		if(barcodes == null) {
			findBarcodes(matchedEltsRead1, matchedEltsRead2);
		}
		return barcodes;
	}
	
	public final void findBarcodes() {
		findBarcodes(null, null);
	}
	
	/**
	 * Identify which of a list of matched elements are barcodes, and append to the barcode sequence for this object
	 * @param readLayout Read layout
	 * @param readElements Matched elements
	 */
	private void findAndAppendBarcodes(ReadLayout readLayout, List<List<ReadSequenceElement>> readElements) {
		if(readElements != null) {
			for(int i = 0; i < readElements.size(); i++) {
				ReadSequenceElement parentElement = readLayout.getElements().get(i);
				Class<? extends ReadSequenceElement> cl = parentElement.getClass();
				if(cl.equals(FragmentBarcode.class) || cl.equals(BarcodeSet.class)) {
					for(ReadSequenceElement elt : readElements.get(i)) {
						barcodes.appendBarcode(((FragmentBarcode)elt).getBarcode());
					}
					continue;
				}
				if(cl.equals(BarcodeEquivalenceClass.class) || cl.equals(BarcodeEquivalenceClassSet.class)) {
					for(ReadSequenceElement elt : readElements.get(i)) {
						BarcodeEquivalenceClass bec = (BarcodeEquivalenceClass) elt;
						barcodes.appendBarcode(bec.toBarcode().getBarcode());
					}
					continue;
				}
			}
		}
	}
	
	/**
	 * Find barcodes where we have already matched elements to the read(s)
	 * @param matchedEltsRead1 Matched elements for read 1 or null if identifying here for the first time or there is no read 1
	 * @param matchedEltsRead2 Matched elements for read 2 or null if identifying here for the first time or there is no read 2
	 */
	public final void findBarcodes(List<List<ReadSequenceElement>> matchedEltsRead1, List<List<ReadSequenceElement>> matchedEltsRead2) {
		barcodes = new BarcodeSequence();
			if(read1layout != null && read1sequence != null) {
				List<List<ReadSequenceElement>> read1elements = 
						matchedEltsRead1 == null ? new BitapMatcher(read1layout, read1sequence).getMatchedElements() : matchedEltsRead1;
				if(read1elements != null) {
					findAndAppendBarcodes(read1layout, read1elements);
				}
				read1elements = null;
			}
			if(read2layout != null && read2sequence != null) {
				List<List<ReadSequenceElement>> read2elements = 
						matchedEltsRead2 == null ? new BitapMatcher(read2layout, read2sequence).getMatchedElements() : matchedEltsRead2;
				if(read2elements != null) {
					findAndAppendBarcodes(read2layout, read2elements);
				}
				read2elements = null;
			}
		setBarcodes(barcodes);
	}
	
	public final String getId() {
		return id;
	}
	
	public final String getUnpairedSequence() {
		return unpairedSequence;
	}
	
	public final String getRead1Sequence() {
		return read1sequence;
	}
	
	public final String getRead2Sequence() {
		return read2sequence;
	}
	
	public final ReadLayout getRead1Layout() {
		return read1layout;
	}
	
	public final ReadLayout getRead2Layout() {
		return read2layout;
	}
	
	public final Annotation getMappedLocation() {
		return location;
	}
	
	/**
	 * Set mapped location of fragment
	 * @param mappedLocation Mapped location
	 */
	public final void setMappedLocation(Annotation mappedLocation) {
		location = mappedLocation;
	}
	
	public final int compareTo(BarcodedFragment other) {
		if(location != null && other.getMappedLocation() != null) {
			int l = location.compareTo(other.getMappedLocation());
			if(l != 0) return l;
		}
		return id.compareTo(other.getId());
	}

	@Override
	public final String getFullInfoString() {
		return location.toBED();
	}
		
	
}
