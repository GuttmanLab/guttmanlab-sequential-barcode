package readlayout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import readelement.Barcode;
import readelement.BarcodeEquivalenceClass;
import readelement.BarcodeEquivalenceClassSet;
import readelement.BarcodeSet;
import readelement.FixedSequence;
import readelement.ReadSequenceElement;
import readelement.FixedSequenceCollection;
import readelement.Switch;

/**
 * Static factory methods for read layouts
 * @author prussell
 *
 */
public class ReadLayoutFactory {

	public static Logger logger = Logger.getLogger(ReadLayoutFactory.class.getName());
	
	/**
	 * Create a read layout for sequential 3D barcoding project with paired end design and ligations of an odd set and even set of barcodes
	 * @param evenBarcodes Even barcodes Table of even barcodes (line format: barcode_ID   barcode_sequence)
	 * @param oddBarcodes Odd barcodes Table of odd barcodes (line format: barcode_ID   barcode_sequence)
	 * @param totalNumBarcodes Total number of barcode ligations
	 * @param rpm RPM sequence
	 * @param readLength Full length of sequencing reads
	 * @param maxMismatchBarcode Max number of mismatches in barcode sequence
	 * @param maxMismatchRpm Max number of mismatches in RPM sequence
	 * @param enforceOddEven Require odd and even barcodes to alternate in read sequences
	 * @return The read layout specified by the parameters
	 * @throws IOException 
	 */
	public static BarcodedReadLayout getRead2LayoutRnaDna3DPairedDesign(String evenBarcodeTableFile, String oddBarcodeTableFile, int totalNumBarcodes, String rpm, int readLength, int maxMismatchBarcode, int maxMismatchRpm, boolean enforceOddEven) throws IOException {
		Collection<Barcode> oddBarcodes = Barcode.createBarcodesFromTable(oddBarcodeTableFile, maxMismatchBarcode);
		Collection<Barcode> evenBarcodes = Barcode.createBarcodesFromTable(evenBarcodeTableFile, maxMismatchBarcode);
		return getRead2LayoutRnaDna3DPairedDesign(evenBarcodes, oddBarcodes, totalNumBarcodes, rpm, readLength, maxMismatchRpm, enforceOddEven);
	}
	
	/**
	 * Create a read layout for sequential 3D barcoding project with single end design and ligations of an odd set and even set of barcodes
	 * Presence of RPM or DPM sequence indicates that the fragment originated from RNA or DNA respectively
	 * @param evenBarcodes Even barcodes Table of even barcodes (line format: barcode_ID   barcode_sequence)
	 * @param oddBarcodes Odd barcodes Table of odd barcodes (line format: barcode_ID   barcode_sequence)
	 * @param totalNumBarcodes Total number of barcode ligations
	 * @param rpm RPM sequence
	 * @param dpm DPM sequence
	 * @param readLength Full length of sequencing reads
	 * @param maxMismatchBarcode Max number of mismatches in barcode sequence
	 * @param maxMismatchRpm Max number of mismatches in RPM sequence
	 * @param maxMismatchDpm Max number of mismatches in DPM sequence
	 * @param enforceOddEven Require odd and even barcodes to alternate in read sequences
	 * @return The read layout specified by the parameters
	 * @throws IOException 
	 */
	public static BarcodedReadLayout getReadLayoutRnaDna3DSingleDesignWithRnaDnaSwitch(String evenBarcodeTableFile, String oddBarcodeTableFile, int totalNumBarcodes, String rpm, String dpm, int readLength, int maxMismatchBarcode, int maxMismatchRpm, int maxMismatchDpm, boolean enforceOddEven) throws IOException {
		Collection<Barcode> oddBarcodes = Barcode.createBarcodesFromTable(oddBarcodeTableFile, maxMismatchBarcode);
		Collection<Barcode> evenBarcodes = Barcode.createBarcodesFromTable(evenBarcodeTableFile, maxMismatchBarcode);
		return getReadLayoutRnaDna3DSingleDesignWithRnaDnaSwitch(evenBarcodes, oddBarcodes, totalNumBarcodes, rpm, dpm, readLength, maxMismatchRpm, maxMismatchDpm, enforceOddEven);
	}
	
	/**
	 * Create a read layout for sequential 3D barcoding project with single end design and ligations of an odd set and even set of barcodes
	 * DPM separates barcodes from rest of read
	 * @param evenBarcodes Even barcodes Table of even barcodes (line format: barcode_ID   barcode_sequence)
	 * @param oddBarcodes Odd barcodes Table of odd barcodes (line format: barcode_ID   barcode_sequence)
	 * @param totalNumBarcodes Total number of barcode ligations
	 * @param dpm DPM sequence
	 * @param readLength Full length of sequencing reads
	 * @param maxMismatchBarcode Max number of mismatches in barcode sequence
	 * @param maxMismatchDpm Max number of mismatches in DPM sequence
	 * @param enforceOddEven Require odd and even barcodes to alternate in read sequences
	 * @return The read layout specified by the parameters
	 * @throws IOException 
	 */
	public static BarcodedReadLayout getReadLayoutRnaDna3DSingleDesign(String evenBarcodeTableFile, String oddBarcodeTableFile, int totalNumBarcodes, String dpm, int readLength, int maxMismatchBarcode, int maxMismatchDpm, boolean enforceOddEven) throws IOException {
		Collection<Barcode> oddBarcodes = Barcode.createBarcodesFromTable(oddBarcodeTableFile, maxMismatchBarcode);
		Collection<Barcode> evenBarcodes = Barcode.createBarcodesFromTable(evenBarcodeTableFile, maxMismatchBarcode);
		return getReadLayoutRnaDna3DSingleDesign(evenBarcodes, oddBarcodes, totalNumBarcodes, dpm, readLength, maxMismatchDpm, enforceOddEven);
	}
	

	/**
	 * Create a read layout for sequential 3D barcoding project with paired end design and ligations of an odd set and even set of barcodes
	 * @param evenBarcodes Even barcodes
	 * @param oddBarcodes Odd barcodes
	 * @param totalNumBarcodes Total number of barcode ligations
	 * @param rpm RPM sequence
	 * @param readLength Full length of sequencing reads
	 * @param maxMismatchRpm Max number of mismatches in RPM sequence
	 * @param enforceOddEven Require odd and even barcodes to alternate in read sequences
	 * @return The read layout specified by the parameters
	 */
	public static BarcodedReadLayout getRead2LayoutRnaDna3DPairedDesign(Collection<Barcode> evenBarcodes, Collection<Barcode> oddBarcodes, int totalNumBarcodes, String rpm, int readLength, int maxMismatchRpm, boolean enforceOddEven) {
		if(enforceOddEven && totalNumBarcodes % 2 != 0) {
			throw new IllegalArgumentException("Total number of barcodes must be even if enforcing odd/even alternation");
		}
		ArrayList<ReadSequenceElement> eltsSequence = new ArrayList<ReadSequenceElement>();
		Collection<Barcode> allBarcodes = new TreeSet<Barcode>();
		allBarcodes.addAll(oddBarcodes);
		allBarcodes.addAll(evenBarcodes);
		
		if(enforceOddEven) {
			BarcodeSet oddBarcodesSet = new BarcodeSet("odd_barcodes", oddBarcodes);
			BarcodeSet evenBarcodesSet = new BarcodeSet("even_barcodes", evenBarcodes);
			for(int i = 0; i < totalNumBarcodes; i++) {
				if(i % 2 == 0) {
					eltsSequence.add(evenBarcodesSet);
				} else {
					eltsSequence.add(oddBarcodesSet);
				}
			}
		} else {
			BarcodeSet allBarcodesSet = new BarcodeSet("all_barcodes", allBarcodes, true, rpm, maxMismatchRpm);
			eltsSequence.add(allBarcodesSet);
		}
		eltsSequence.add(new FixedSequence("rpm", rpm, maxMismatchRpm));
		return new BarcodedReadLayout(eltsSequence, readLength);
	}

	
	/**
	 * Create a read layout for sequential 3D barcoding project with single end design and ligations of an odd set and even set of barcodes
	 * Presence of RPM or DPM sequence indicates that the fragment originated from RNA or DNA respectively
	 * @param evenBarcodes Even barcodes
	 * @param oddBarcodes Odd barcodes
	 * @param totalNumBarcodes Total number of barcode ligations
	 * @param rpm RPM sequence
	 * @param dpm DPM sequence
	 * @param readLength Full length of sequencing reads
	 * @param maxMismatchRpm Max number of mismatches in RPM sequence
	 * @param maxMismatchDpm Max number of mismatches in DPM sequence
	 * @param enforceOddEven Require odd and even barcodes to alternate in read sequences
	 * @return The read layout specified by the parameters
	 */
	public static BarcodedReadLayout getReadLayoutRnaDna3DSingleDesignWithRnaDnaSwitch(Collection<Barcode> evenBarcodes, Collection<Barcode> oddBarcodes, int totalNumBarcodes, String rpm, String dpm, int readLength, int maxMismatchRpm, int maxMismatchDpm, boolean enforceOddEven) {
		if(enforceOddEven && totalNumBarcodes % 2 != 0) {
			throw new IllegalArgumentException("Total number of barcodes must be even if enforcing odd/even alternation");
		}
		ArrayList<ReadSequenceElement> eltsSequence = new ArrayList<ReadSequenceElement>();
		
		// Add barcodes
		Collection<Barcode> allBarcodes = new TreeSet<Barcode>();
		allBarcodes.addAll(oddBarcodes);
		allBarcodes.addAll(evenBarcodes);
		
		if(enforceOddEven) {
			BarcodeSet oddBarcodesSet = new BarcodeSet("odd_barcodes", oddBarcodes);
			BarcodeSet evenBarcodesSet = new BarcodeSet("even_barcodes", evenBarcodes);
			for(int i = 0; i < totalNumBarcodes; i++) {
				if(i % 2 == 0) {
					eltsSequence.add(evenBarcodesSet);
				} else {
					eltsSequence.add(oddBarcodesSet);
				}
			}
		} else {
			BarcodeSet allBarcodesSet = new BarcodeSet("all_barcodes", allBarcodes, true, rpm, maxMismatchRpm);
			eltsSequence.add(allBarcodesSet);
		}
		
		// Add switch for RPM/DPM
		eltsSequence.add(new Switch("rpm", rpm, maxMismatchRpm, "dpm", dpm, maxMismatchDpm));
		
		
		return new BarcodedReadLayout(eltsSequence, readLength);
	}
	
	
	/**
	 * Create a read layout for sequential 3D barcoding project with single end design and ligations of an odd set and even set of barcodes
	 * DPM separates barcodes from rest of read
	 * @param evenBarcodes Even barcodes
	 * @param oddBarcodes Odd barcodes
	 * @param totalNumBarcodes Total number of barcode ligations
	 * @param dpm DPM sequence
	 * @param readLength Full length of sequencing reads
	 * @param maxMismatchDpm Max number of mismatches in DPM sequence
	 * @param enforceOddEven Require odd and even barcodes to alternate in read sequences
	 * @return The read layout specified by the parameters
	 */
	public static BarcodedReadLayout getReadLayoutRnaDna3DSingleDesign(Collection<Barcode> evenBarcodes, Collection<Barcode> oddBarcodes, int totalNumBarcodes, String dpm, int readLength, int maxMismatchDpm, boolean enforceOddEven) {
		if(enforceOddEven && totalNumBarcodes % 2 != 0) {
			throw new IllegalArgumentException("Total number of barcodes must be even if enforcing odd/even alternation");
		}
		ArrayList<ReadSequenceElement> eltsSequence = new ArrayList<ReadSequenceElement>();
		
		// Add barcodes
		Collection<Barcode> allBarcodes = new TreeSet<Barcode>();
		allBarcodes.addAll(oddBarcodes);
		allBarcodes.addAll(evenBarcodes);
		
		if(enforceOddEven) {
			BarcodeSet oddBarcodesSet = new BarcodeSet("odd_barcodes", oddBarcodes);
			BarcodeSet evenBarcodesSet = new BarcodeSet("even_barcodes", evenBarcodes);
			for(int i = 0; i < totalNumBarcodes; i++) {
				if(i % 2 == 0) {
					eltsSequence.add(evenBarcodesSet);
				} else {
					eltsSequence.add(oddBarcodesSet);
				}
			}
		} else {
			BarcodeSet allBarcodesSet = new BarcodeSet("all_barcodes", allBarcodes, true, dpm, maxMismatchDpm);
			eltsSequence.add(allBarcodesSet);
		}
		
		// Add switch for DPM
		eltsSequence.add(new FixedSequence("dpm", dpm, maxMismatchDpm));
		
		
		return new BarcodedReadLayout(eltsSequence, readLength);
	}
	
	
	/**
	 * Create a read layout for sequential 3D barcoding project with single end design and ligations of an odd set and even set of barcodes
	 * A repeatable set of adapters separate barcodes from rest of read
	 * @param evenBarcodeTableFile File with list of even barcodes
	 * @param oddBarcodeTableFile File with list of odd barcodes
	 * @param totalNumBarcodes Total number of barcode ligations
	 * @param adapterSeqFasta Fasta file of adapter sequences that come between barcodes and nucleotide sequence
	 * @param readLength Full length of sequencing reads
	 * @param maxMismatchBarcode Max number of mismatches in barcode sequence
	 * @param maxMismatchAdapter Max number of mismatches in adapter sequence
	 * @param enforceOddEven Require odd and even barcodes to alternate in read sequences
	 * @return The read layout specified by the parameters
	 * @throws IOException 
	 */
	public static BarcodedReadLayout getReadLayoutRnaDna3DSingleDesignMarch2015(String evenBarcodeTableFile, String oddBarcodeTableFile, int totalNumBarcodes, String adapterSeqFasta, int readLength, int maxMismatchBarcode, int maxMismatchAdapter, boolean enforceOddEven) throws IOException {
		Collection<Barcode> oddBarcodes = Barcode.createBarcodesFromTable(oddBarcodeTableFile, maxMismatchBarcode);
		Collection<Barcode> evenBarcodes = Barcode.createBarcodesFromTable(evenBarcodeTableFile, maxMismatchBarcode);
		return getReadLayoutRnaDna3DSingleDesignMarch2015(evenBarcodes, oddBarcodes, totalNumBarcodes, adapterSeqFasta, readLength, maxMismatchAdapter, enforceOddEven);
	}
	
	/**
	 * Create a read layout for sequential 3D barcoding project with single end design and ligations of an odd set and even set of barcodes
	 * A repeatable set of adapters separate barcodes from rest of read
	 * @param evenBarcodes Even barcodes
	 * @param oddBarcodes Odd barcodes
	 * @param totalNumBarcodes Total number of barcode ligations
	 * @param adapterSeqFasta Fasta file of adapter sequences that come between barcodes and nucleotide sequence
	 * @param readLength Full length of sequencing reads
	 * @param maxMismatchAdapter Max number of mismatches in adapter sequence
	 * @param enforceOddEven Require odd and even barcodes to alternate in read sequences
	 * @return The read layout specified by the parameters
	 */
	public static BarcodedReadLayout getReadLayoutRnaDna3DSingleDesignMarch2015(Collection<Barcode> evenBarcodes, Collection<Barcode> oddBarcodes, int totalNumBarcodes, String adapterSeqFasta, int readLength, int maxMismatchAdapter, boolean enforceOddEven) {

		FixedSequenceCollection adapters = new FixedSequenceCollection(adapterSeqFasta, maxMismatchAdapter, true);
		
		if(enforceOddEven && totalNumBarcodes % 2 != 0) {
			throw new IllegalArgumentException("Total number of barcodes must be even if enforcing odd/even alternation");
		}
		ArrayList<ReadSequenceElement> eltsSequence = new ArrayList<ReadSequenceElement>();
		
		// Add barcodes
		Collection<Barcode> allBarcodes = new TreeSet<Barcode>();
		allBarcodes.addAll(oddBarcodes);
		allBarcodes.addAll(evenBarcodes);
		
		if(enforceOddEven) {
			BarcodeSet oddBarcodesSet = new BarcodeSet("odd_barcodes", oddBarcodes);
			BarcodeSet evenBarcodesSet = new BarcodeSet("even_barcodes", evenBarcodes);
			for(int i = 0; i < totalNumBarcodes; i++) {
				if(i % 2 == 0) {
					eltsSequence.add(evenBarcodesSet);
				} else {
					eltsSequence.add(oddBarcodesSet);
				}
			}
		} else {
			BarcodeSet allBarcodesSet = new BarcodeSet("all_barcodes", allBarcodes, true, adapters);
			eltsSequence.add(allBarcodesSet);
		}
		
		// Add switches for adapters
		eltsSequence.add(adapters);
		
		
		return new BarcodedReadLayout(eltsSequence, readLength);
	}
	

	/**
	 * Read layout:
	 * An extra barcode appears at beginning of read
	 * Next, a series of barcodes that fall into equivalence classes
	 * 		Classes of 4 barcodes each are considered a single barcode
	 * Finally, another barcode
	 * No fixed sequence separates barcodes from DNA
	 * @param firstBarcodeTableFile File with list of first barcode
	 * @param barcodeEquivClassFile Barcode equivalence class file as defined in static method that reads it
	 * @param lastBarcodeTableFile File with list of last barcode
	 * @param readLength Read length
	 * @param maxMismatchFirstBarcode
	 * @param maxMismatchBarcodeEquivClass Max mismatches for barcode equivalence classes
	 * @param maxMismatchLastBarcode Max mismatches for last barcode
	 * @return
	 * @throws IOException
	 */
	public static BarcodedReadLayout getReadLayoutRnaDna3DSingleDesignMay2015(String firstBarcodeTableFile, String barcodeEquivClassFile, String lastBarcodeTableFile, int readLength, int maxMismatchFirstBarcode, int maxMismatchBarcodeEquivClass, int maxMismatchLastBarcode) throws IOException {
		
		Collection<Barcode> firstBarcode = Barcode.createBarcodesFromTable(firstBarcodeTableFile, maxMismatchFirstBarcode);
		Collection<BarcodeEquivalenceClass> barcodeEquivClasses = BarcodeEquivalenceClass.createEquivClassesFromTable(barcodeEquivClassFile, maxMismatchBarcodeEquivClass);
		Collection<Barcode> lastBarcode = Barcode.createBarcodesFromTable(lastBarcodeTableFile, maxMismatchLastBarcode);
		
		return getReadLayoutRnaDna3DSingleDesignMay2015(firstBarcode, barcodeEquivClasses, lastBarcode, readLength, maxMismatchBarcodeEquivClass, maxMismatchLastBarcode); 
		
	}


	/**
	 * Read layout:
	 * An extra barcode appears at beginning of read
	 * Next, a series of barcodes that fall into equivalence classes
	 * 		Classes of 4 barcodes each are considered a single barcode
	 * Finally, another barcode
	 * No fixed sequence separates barcodes from DNA
	 * @param firstBarcode Options for the first 8 or 9mer
	 * @param barcodeEquivClasses The barcode equivalence classes
	 * @param lastBarcode Options for the last barcode
	 * @param readLength Read length
	 * @param maxMismatchBarcodeEquivClass Max mismatches for barcode equivalence classes
	 * @param maxMismatchLastBarcode Max mismatches for last barcode
	 * @return
	 */
	public static BarcodedReadLayout getReadLayoutRnaDna3DSingleDesignMay2015(Collection<Barcode> firstBarcode, Collection<BarcodeEquivalenceClass> barcodeEquivClasses, Collection<Barcode> lastBarcode, int readLength, int maxMismatchBarcodeEquivClass, int maxMismatchLastBarcode) {
		
		FixedSequenceCollection stopSignalFirstBarcode = equivClassesAsFixedSeqs(barcodeEquivClasses, maxMismatchBarcodeEquivClass, true);
		FixedSequenceCollection stopSignalBarcodeEquivClasses = barcodesAsFixedSeqs(lastBarcode, maxMismatchLastBarcode, false);
		
		BarcodeSet firstBarcodeSet = new BarcodeSet("first_barcode", firstBarcode, false, stopSignalFirstBarcode);
		BarcodeEquivalenceClassSet barcodeEquivClassesSet = new BarcodeEquivalenceClassSet("barcode_equiv_classes", barcodeEquivClasses, true, stopSignalBarcodeEquivClasses);
		BarcodeSet lastBarcodeSet = new BarcodeSet("last_barcode", lastBarcode, false);
		
		ArrayList<ReadSequenceElement> eltsSequence = new ArrayList<ReadSequenceElement>();
		eltsSequence.add(firstBarcodeSet);
		eltsSequence.add(barcodeEquivClassesSet);
		eltsSequence.add(lastBarcodeSet);
		
		return new BarcodedReadLayout(eltsSequence, readLength);
		
	}
	
	/**
	 * Convert the barcodes to fixed sequence objects
	 * @param barcodes Barcodes
	 * @param maxMismatch Max mismatches per barcode
	 * @param isRepeatable Whether the final collection is repeatable
	 * @return FixedSequenceCollection object containing the barcode sequences
	 */
	private static FixedSequenceCollection barcodesAsFixedSeqs(Collection<Barcode> barcodes, int maxMismatch, boolean isRepeatable) {
		Collection<FixedSequence> seqs = new ArrayList<FixedSequence>();
		for(Barcode barcode : barcodes) {
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
	private static FixedSequenceCollection equivClassesAsFixedSeqs(Collection<BarcodeEquivalenceClass> barcodes, int maxMismatch, boolean isRepeatable) {
		Collection<FixedSequence> seqs = new ArrayList<FixedSequence>();
		for(BarcodeEquivalenceClass ec : barcodes) {
			Collection<Barcode> ecBarcodes = ec.getBarcodes();
			for(Barcode barcode : ecBarcodes) {
				seqs.add(new FixedSequence(barcode.getId(), barcode.getSequence(), maxMismatch));
			}
		}
		return new FixedSequenceCollection(seqs, isRepeatable);
	}
	
}


