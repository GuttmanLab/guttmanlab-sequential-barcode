package readlayout;

import java.util.ArrayList;
import java.util.Collection;
import java.util.TreeSet;

import readelement.FragmentBarcode;
import readelement.BarcodeSet;
import readelement.ReadSequenceElement;

/**
 * A read layout including barcodes
 * @author prussell
 *
 */
public class BarcodedReadLayout extends ReadLayout {
	
	private Collection<FragmentBarcode> barcodes;
	
	/**
	 * @param elementSequence Sequence of read elements
	 * @param readLength Read length
	 */
	public BarcodedReadLayout(ArrayList<ReadSequenceElement> elementSequence, int readLength) {
		super(elementSequence, readLength);
		initializeBarcodes();
	}
	
	/**
	 * Initialize the barcodes based on the read elements
	 */
	private void initializeBarcodes() {
		barcodes = new TreeSet<FragmentBarcode>();
		ArrayList<ReadSequenceElement> elements = getElements();
		if(elements != null) {
			for(int i = 0; i < elements.size(); i++) {
				ReadSequenceElement parentElement = getElements().get(i);
				if(parentElement.getClass().equals(FragmentBarcode.class)) {
					FragmentBarcode barcode = (FragmentBarcode) parentElement;
					barcodes.add(barcode);
					continue;
				}
				if(parentElement.getClass().equals(BarcodeSet.class)) {
					BarcodeSet barcodeSet = (BarcodeSet) parentElement;
					barcodes.addAll(barcodeSet.getBarcodes());
					continue;
				}
			}
		}
	}
	
	public Collection<FragmentBarcode> getAllBarcodes() {
		return barcodes;
	}
	
}
