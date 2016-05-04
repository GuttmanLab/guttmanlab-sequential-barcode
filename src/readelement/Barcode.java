package readelement;

import guttmanlab.core.util.FileUtil;
import guttmanlab.core.util.StringParser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.TreeSet;


/**
 * A single barcode
 * Instances are immutable
 * @author prussell
 *
 */
public final class Barcode implements Comparable<Barcode> {
	
	private String sequence;
	private String id;

		/**
	 * @param seq The barcode sequence
	 */
	public Barcode(String seq) {
		this(seq, null);
	}
	
	/**
	 * @param seq The barcode sequence
	 * @param barcodeId Unique ID for this barcode
	 */
	public Barcode(String seq, String barcodeId) {
		sequence = seq;
		id = barcodeId;
	}
	
	/**
	 * Create a set of barcodes with IDs from a table file
	 * Line format: barcode_id	barcode_sequence
	 * @param tableFile Table file
	 * @return Collection of barcodes
	 * @throws IOException
	 */
	public static Collection<Barcode> createBarcodesFromTable(String tableFile) throws IOException {
		FileReader r = new FileReader(tableFile);
		BufferedReader b = new BufferedReader(r);
		StringParser s = new StringParser();
		Collection<Barcode> rtrn = new TreeSet<Barcode>();
		while(b.ready()) {
			s.parse(b.readLine());
			if(s.getFieldCount() == 0) {
				continue;
			}
			if(s.getFieldCount() != 2) {
				r.close();
				b.close();
				throw new IllegalArgumentException("Format: barcode_id  barcode_sequence");
			}
			rtrn.add(new Barcode(s.asString(1), s.asString(0)));
		}
		r.close();
		b.close();
		return rtrn;
	}

	/**
	 * Create a set of barcodes without IDs from a list file
	 * @param listFile File with one barcode sequence per line
	 * @return Collection of barcodes
	 * @throws IOException
	 */
	public static Collection<Barcode> createBarcodesFromList(String listFile) throws IOException {
		return createBarcodes(FileUtil.fileLinesAsList(listFile));
	}
	
	/**
	 * Create barcode objects from a collection of barcode sequences
	 * @param barcodeSeqs Barcode sequences
	 * @return Collection of barcode objects
	 */
	public static Collection<Barcode> createBarcodes(Collection<String> barcodeSeqs) {
		Collection<Barcode> rtrn = new TreeSet<Barcode>();
		for(String b : barcodeSeqs) {
			rtrn.add(new Barcode(b));
		}
		return rtrn;
	}
	
	@Override
	public int compareTo(Barcode o) {
		int c = sequence.compareTo(o.getSequence());
		if(c != 0 || id == null) {
			return c;
		}
		return id.compareTo(o.getId());
	}

	/**
	 * Get the barcode ID
	 * @return The barcode ID
	 */
	public String getId() {
		return id;
	}
	
	/**
	 * Get the barcode sequence
	 * @return The barcode sequence
	 */
	public String getSequence() {
		return sequence;
	}

	/**
	 * Get the length of the sequence
	 * @return Length of barcode sequence
	 */
	public int getLength() {
		return sequence.length();
	}
	
	@Override
	public String toString() {
		return id + ":" + sequence;
	}
	
	@Override
	public boolean equals(Object o) {
		if(!o.getClass().equals(Barcode.class)) return false;
		Barcode b = (Barcode)o;
		return toString().equals(b.toString());
	}
	
	@Override
	public int hashCode() {
		return toString().hashCode();
	}
	

}
