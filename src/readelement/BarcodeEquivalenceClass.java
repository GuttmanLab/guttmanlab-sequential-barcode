package readelement;

import guttmanlab.core.util.StringParser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * An equivalence class of barcodes
 * A sequence that matches any one of the barcodes is considered to match the whole set
 * @author prussell
 *
 */
public class BarcodeEquivalenceClass extends BarcodeSet {
	
	/**
	 * @param setId Barcode set ID
	 * @param barcodeSet The barcodes
	 */
	public BarcodeEquivalenceClass(String setId, Collection<Barcode> barcodeSet) {
		super(setId, barcodeSet);
	}
			
	/**
	 * @param setId Barcode set ID
	 * @param barcodeSet The barcodes
	 * @param isRepeatable Whether to look for multiple matches in sequence
	 * @param stopSignalForRepeatable String whose presence in a read signals the end of the region that is expected to contain these barcodes
	 */
	public BarcodeEquivalenceClass(String setId, Collection<Barcode> barcodeSet, boolean isRepeatable) {
		super(setId, barcodeSet, isRepeatable);
	}
	
	/**
	 * @param setId Barcode set ID
	 * @param barcodeSet The barcodes
	 * @param isRepeatable Whether to look for multiple matches in sequence
	 * @param stopSignal String whose presence in a read signals the end of the region that is expected to contain these barcodes
	 * @param stopSignalMaxMismatch Max mismatches to count a match for stop signal
	 */
	public BarcodeEquivalenceClass(String setId, Collection<Barcode> barcodeSet, boolean isRepeatable, String stopSignal, int stopSignalMaxMismatch) {
		super(setId, barcodeSet, isRepeatable, stopSignal, stopSignalMaxMismatch);
	}
	
	/**
	 * @param setId Barcode set ID
	 * @param barcodeSet The barcodes
	 * @param isRepeatable Whether to look for multiple matches in sequence
	 * @param stopSignal Collection of strings whose presence in a read signals the end of the region that is expected to contain these barcodes
	 */
	public BarcodeEquivalenceClass(String setId, Collection<Barcode> barcodeSet, boolean isRepeatable, FixedSequenceCollection stopSignal) {
		super(setId, barcodeSet, isRepeatable, stopSignal);
	}

	/**
	 * Get a Barcode object for printing info only
	 * @return Dummy barcode object with this ID
	 */
	public Barcode toBarcode() {
		return new Barcode("NA", getId());
	}
	
	@Override
	public String elementName() {
		return "barcode_equivalence_class";
	}

	/**
	 * Create a collection of equivalence classes from table of barcodes
	 * Line format: equiv_class barcode_id barcode_seq
	 * @param barcodeEquivClassFile Equivalence class file
	 * @param maxMismatchBarcodeEquivClass Max mismatches when identifying barcodes
	 * @return Collection of equivalence classes specified in the file
	 * @throws IOException 
	 */
	public static Collection<BarcodeEquivalenceClass> createEquivClassesFromTable(String barcodeEquivClassFile, int maxMismatchBarcodeEquivClass) throws IOException {
		
		Map<String, Map<String, String>> allClasses = new HashMap<String, Map<String, String>>();
		BufferedReader reader = new BufferedReader(new FileReader(barcodeEquivClassFile));
		StringParser s = new StringParser();
		while(reader.ready()) {
			s.parse(reader.readLine());
			if(s.getFieldCount() != 3) {
				reader.close();
				throw new IllegalArgumentException("Line format: equiv_class barcode_id barcode_seq");
			}
			String ec = s.asString(0);
			String id = s.asString(1);
			String seq = s.asString(2);
			if(!allClasses.containsKey(ec)) {
				allClasses.put(ec, new HashMap<String, String>());
			}
			allClasses.get(ec).put(id, seq);
		}
		reader.close();
		
		Collection<BarcodeEquivalenceClass> rtrn = new ArrayList<BarcodeEquivalenceClass>();
		for(String ec : allClasses.keySet()) {
			Collection<Barcode> barcodes = new ArrayList<Barcode>();
			for(String id : allClasses.get(ec).keySet()) {
				barcodes.add(new Barcode(allClasses.get(ec).get(id), id, maxMismatchBarcodeEquivClass));
			}
			rtrn.add(new BarcodeEquivalenceClass(ec, barcodes));
		}
		return rtrn;
		
	}
	
	@Override
	public Map<String, ReadSequenceElement> sequenceToElement() {
		Map<String, ReadSequenceElement> rtrn = new HashMap<String, ReadSequenceElement>();
		for(Barcode barcode : barcodes) {
			rtrn.put(barcode.getSequence(), this);
		}
		return rtrn;
	}

	@Override
	public int minMatch() {
		throw new UnsupportedOperationException("NA");
	}


}
