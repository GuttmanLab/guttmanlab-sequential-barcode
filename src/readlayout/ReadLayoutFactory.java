package readlayout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.log4j.Logger;

import readelement.FragmentBarcode;
import readelement.BarcodeEquivalenceClass;
import readelement.BarcodeSet;
import readelement.FixedSequence;
import readelement.ReadSequenceElement;
import readelement.FixedSequenceCollection;

/**
 * Static factory methods for read layouts
 * @author prussell
 *
 */
public final class ReadLayoutFactory {

	public static Logger logger = Logger.getLogger(ReadLayoutFactory.class.getName());
	
	
	/**
	 * April 2016 design
	 * RPM or DPM is in read 1, barcodes are in read 2
	 * @param evenBarcodesFile Even barcodes list file
	 * @param oddBarcodesFile Odd barcodes list file
	 * @param yShapeBarcodesFile Y shape barcodes list file
	 * @param maxMismatchOddEven Max mismatch in odd/even barcodes
	 * @param maxMismatchYshape Max mismatch in Y shape barcode
	 * @param read2length Read 2 length
	 * @param totalNumBarcodes 4 or 5
	 * @return Read 2 layout
	 */
	public static BarcodedReadLayout getRead2LayoutRnaDna3DPairedDesignApril2016(String evenBarcodesFile, String oddBarcodesFile, 
			String yShapeBarcodesFile, int maxMismatchOddEven, int maxMismatchYshape, int read2length, int totalNumBarcodes) throws IOException {
		Collection<FragmentBarcode> oddBarcodes = FragmentBarcode.createBarcodesFromTable(oddBarcodesFile, maxMismatchOddEven);
		Collection<FragmentBarcode> evenBarcodes = FragmentBarcode.createBarcodesFromTable(evenBarcodesFile, maxMismatchOddEven);
		Collection<FragmentBarcode> yBarcodes = FragmentBarcode.createBarcodesFromTable(yShapeBarcodesFile, maxMismatchYshape);
		if(totalNumBarcodes == 4) return getRead2LayoutRnaDna3DPairedDesignApril2016_4barcode(evenBarcodes, oddBarcodes, yBarcodes, read2length);
		if(totalNumBarcodes == 5) return getRead2LayoutRnaDna3DPairedDesignApril2016_5barcode(evenBarcodes, oddBarcodes, yBarcodes, read2length);
		throw new IllegalArgumentException("Total barcodes must be 4 or 5");
	}

	/**
	 * April 2016 design with 5 barcodes
	 * RPM or DPM is in read 1, barcodes are in read 2
	 * @param evenBarcodes Even barcodes
	 * @param oddBarcodes Odd barcodes
	 * @param yShapeBarcodes Y shape barcodes
	 * @param read2length Read 2 length
	 * @return Read 2 layout
	 */
	public static BarcodedReadLayout getRead2LayoutRnaDna3DPairedDesignApril2016_5barcode(Collection<FragmentBarcode> evenBarcodes, 
			Collection<FragmentBarcode> oddBarcodes, Collection<FragmentBarcode> yShapeBarcodes, int read2length) {
		ArrayList<ReadSequenceElement> eltsSequence = new ArrayList<ReadSequenceElement>();
		BarcodeSet yShapeBarcodesSet = new BarcodeSet("y_shape_barcodes", yShapeBarcodes);
		BarcodeSet oddBarcodesSet = new BarcodeSet("odd_barcodes", oddBarcodes);
		BarcodeSet evenBarcodesSet = new BarcodeSet("even_barcodes", evenBarcodes);
		eltsSequence.add(yShapeBarcodesSet);
		eltsSequence.add(evenBarcodesSet);
		eltsSequence.add(oddBarcodesSet);
		eltsSequence.add(evenBarcodesSet);
		eltsSequence.add(oddBarcodesSet);
		return new BarcodedReadLayout(eltsSequence, read2length);
	}
	
	/**
	 * April 2016 design with 4 barcodes
	 * RPM or DPM is in read 1, barcodes are in read 2
	 * @param evenBarcodes Even barcodes
	 * @param oddBarcodes Odd barcodes
	 * @param yShapeBarcodes Y shape barcodes
	 * @param read2length Read 2 length
	 * @return Read 2 layout
	 */
	public static BarcodedReadLayout getRead2LayoutRnaDna3DPairedDesignApril2016_4barcode(Collection<FragmentBarcode> evenBarcodes, 
			Collection<FragmentBarcode> oddBarcodes, Collection<FragmentBarcode> yShapeBarcodes, int read2length) {
		ArrayList<ReadSequenceElement> eltsSequence = new ArrayList<ReadSequenceElement>();
		BarcodeSet yShapeBarcodesSet = new BarcodeSet("y_shape_barcodes", yShapeBarcodes);
		BarcodeSet oddBarcodesSet = new BarcodeSet("odd_barcodes", oddBarcodes);
		BarcodeSet evenBarcodesSet = new BarcodeSet("even_barcodes", evenBarcodes);
		eltsSequence.add(yShapeBarcodesSet);
		eltsSequence.add(evenBarcodesSet);
		eltsSequence.add(oddBarcodesSet);
		eltsSequence.add(evenBarcodesSet);
		return new BarcodedReadLayout(eltsSequence, read2length);
	}
	
	/**
	 * Convert the barcodes to fixed sequence objects
	 * @param barcodes Barcodes
	 * @param maxMismatch Max mismatches per barcode
	 * @param isRepeatable Whether the final collection is repeatable
	 * @return FixedSequenceCollection object containing the barcode sequences
	 */
	@SuppressWarnings("unused")
	private static FixedSequenceCollection barcodesAsFixedSeqs(Collection<FragmentBarcode> barcodes, int maxMismatch, boolean isRepeatable) {
		Collection<FixedSequence> seqs = new ArrayList<FixedSequence>();
		for(FragmentBarcode barcode : barcodes) {
			seqs.add(new FixedSequence(barcode.getId(), barcode.getSequence(), maxMismatch));
		}
		return new FixedSequenceCollection(seqs, isRepeatable);
	}
	
	/**
	 * Convert the barcodes to fixed sequence objects
	 * @param barcodes Barcodes
	 * @param maxMismatch Max mismatches per barcode
	 * @param isRepeatable Whether the final collection is repeatable
	 * @return FixedSequenceCollection object containing the barcode sequences
	 */
	@SuppressWarnings("unused")
	private static FixedSequenceCollection equivClassesAsFixedSeqs(Collection<BarcodeEquivalenceClass> barcodes, int maxMismatch, boolean isRepeatable) {
		Collection<FixedSequence> seqs = new ArrayList<FixedSequence>();
		for(BarcodeEquivalenceClass ec : barcodes) {
			Collection<FragmentBarcode> ecBarcodes = ec.getBarcodes();
			for(FragmentBarcode barcode : ecBarcodes) {
				seqs.add(new FixedSequence(barcode.getId(), barcode.getSequence(), maxMismatch));
			}
		}
		return new FixedSequenceCollection(seqs, isRepeatable);
	}
	
}


