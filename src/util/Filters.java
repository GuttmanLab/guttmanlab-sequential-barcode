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
