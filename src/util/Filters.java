package util;

import guttmanlab.core.serialize.sam.AvroSamRecord;

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Predicate;

import net.sf.samtools.SAMRecord;

public class Filters {

	/**
	 * Get whether a record passes all of a collection of filters
	 * @param record The record
	 * @param filters The filters
	 * @return True if the record passes all filters or the collection of filters is empty
	 */
	public static <T extends Object> boolean passesAll(T record, Collection<Predicate<T>> filters) {
		return filters
				.stream()
				.reduce(Predicate::and)
				.orElse(x -> true)
				.test(record);
	}

	/**
	 * Test whether a record has all barcodes according to the custom tag
	 */
	public static final Predicate<SAMRecord> hasAllBarcodes = record -> {
		String attribute = record.getStringAttribute(CustomSamTag.HAS_ALL_BARCODES);
		if(attribute == null) return true;
		return attribute.equals("T");
	};
	
	private static void validateMoleculeTagVal(String tagVal) {
		if(!tagVal.equals(CustomSamTag.RNA_DNA_VAL_DNA) && !tagVal.equals(CustomSamTag.RNA_DNA_VAL_RNA)) {
			throw new IllegalArgumentException("Illegal tag value: " + tagVal + ". Options: " + CustomSamTag.RNA_DNA_VAL_DNA + " " + CustomSamTag.RNA_DNA_VAL_RNA);
		}
	}
	
	/**
	 * Get a predicate to test whether a record is marked as RNA or DNA
	 * @param tagVal Value of tag to test for. Allowable options: RNA value or DNA value
	 * @return The predicate
	 */
	private static final Predicate<SAMRecord> moleculeTypeReadPredicate(String tagVal) {
		validateMoleculeTagVal(tagVal);
		return record -> {
			String attribute = record.getStringAttribute(CustomSamTag.RNA_DNA);
			if(attribute == null) {
				throw new IllegalArgumentException("Record does not have " + CustomSamTag.RNA_DNA + " tag: " + record.toString());
			}
			return attribute.equals(tagVal);
		};
	}
	
	/**
	 * Get a predicate to test whether a record is marked as RNA or DNA
	 * @param tagVal Value of tag to test for. Allowable options: RNA value or DNA value
	 * @return The predicate
	 */
	private static final Predicate<AvroSamRecord> moleculeTypeAvroPredicate(String tagVal) {
		validateMoleculeTagVal(tagVal);
		return record -> {
			String attribute = record.getAttribute(CustomSamTag.RNA_DNA).toString();
			if(attribute == null) {
				throw new IllegalArgumentException("Record does not have " + CustomSamTag.RNA_DNA + " tag: " + record.toString());
			}
			return attribute.equals(tagVal);
		};
	}
	
	/**
	 * Test whether a record is marked as RNA
	 */
	public static final Predicate<SAMRecord> readIsRNA = moleculeTypeReadPredicate(CustomSamTag.RNA_DNA_VAL_RNA);
	
	/**
	 * Test whether a record is marked as DNA
	 */
	public static final Predicate<SAMRecord> readIsDNA = moleculeTypeReadPredicate(CustomSamTag.RNA_DNA_VAL_DNA);
	
	/**
	 * Test whether a record is marked as RNA
	 */
	public static final Predicate<AvroSamRecord> recordIsRNA = moleculeTypeAvroPredicate(CustomSamTag.RNA_DNA_VAL_RNA);
	
	/**
	 * Test whether a record is marked as DNA
	 */
	public static final Predicate<AvroSamRecord> recordIsDNA = moleculeTypeAvroPredicate(CustomSamTag.RNA_DNA_VAL_DNA);
	
	/**
	 * Get the default avro record filters
	 * @return Default avro record filters
	 */
	public static final Collection<Predicate<AvroSamRecord>> defaultAvroFilters() {
		Collection<Predicate<AvroSamRecord>> rtrn = new ArrayList<Predicate<AvroSamRecord>>();
		return rtrn;
	}
	
	/**
	 * Get the default SAM record filters
	 * @return Default SAM record filters
	 */
	public static final Collection<Predicate<SAMRecord>> defaultSamFilters() {
		Collection<Predicate<SAMRecord>> rtrn = new ArrayList<Predicate<SAMRecord>>();
		rtrn.add(hasAllBarcodes);
		return rtrn;
	}

}
