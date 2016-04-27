package util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import readelement.BarcodeSet;
import readelement.FragmentBarcode;
import readelement.ReadSequenceElement;
import readlayout.BarcodedReadLayout;
import guttmanlab.core.pipeline.ConfigFile;
import guttmanlab.core.pipeline.ConfigFileOption;
import guttmanlab.core.pipeline.ConfigFileOptionValue;
import guttmanlab.core.pipeline.ConfigFileSection;

/**
 * A config file for barcode identification
 * @author prussell
 *
 */
public class BarcodeAnalysisConfigFile extends ConfigFile {
		
	
	/***************************************************************************************
	 *                           Options to describe the reads
	 ***************************************************************************************/
	
	private static final ConfigFileOption optionUnpairedFastq = new ConfigFileOption("unpaired_fastq", "Fastq file of unpaired reads",
			2, false, false, false);
	
	private static final ConfigFileOption optionRead1Fastq = new ConfigFileOption("read1_fastq", "Fastq file of read 1",
			2, false, false, false);
	
	private static final ConfigFileOption optionRead2Fastq = new ConfigFileOption("read2_fastq", "Fastq file of read 2",
			2, false, false, false);
	
	private static final ConfigFileOption optionUnpairedReadLength = new ConfigFileOption("unpaired_read_length", "Length of unpaired reads",
			2, false, false, false);
	
	private static final ConfigFileOption optionRead1Length = new ConfigFileOption("read1_length", "Length of read 1",
			2, false, false, false);
	
	private static final ConfigFileOption optionRead2Length = new ConfigFileOption("read2_length", "Length of read 2",
			2, false, false, false);
	
	private static final Collection<ConfigFileOption> optionsFastq() {
		Collection<ConfigFileOption> rtrn = new ArrayList<ConfigFileOption>();
		rtrn.add(optionUnpairedFastq);
		rtrn.add(optionRead1Fastq);
		rtrn.add(optionRead2Fastq);
		rtrn.add(optionUnpairedReadLength);
		rtrn.add(optionRead1Length);
		rtrn.add(optionRead2Length);
		return rtrn;
	}
	
	private static final ConfigFileSection sectionFastq = new ConfigFileSection("Data", optionsFastq(), true);
	

	/***************************************************************************************
	 *                                Run parameters
	 ***************************************************************************************/
	
	private static final ConfigFileOption optionOutputPrefix = new ConfigFileOption("output_prefix", "Output file prefix", 2, false, false, true);
	private static final ConfigFileOption optionOutputSuffixFastqUnpaired = new ConfigFileOption("output_suffix_fq_unpaired", 
			"Output suffix fastq for unpaired reads (the part of the read after identified barcodes)", 2, false, false, false);
	private static final ConfigFileOption optionOutputSuffixFastq1 = new ConfigFileOption("output_suffix_fq_1", 
			"Output suffix fastq for read1 (the part of the read after identified barcodes)", 2, false, false, false);
	private static final ConfigFileOption optionOutputSuffixFastq2 = new ConfigFileOption("output_suffix_fq_2", 
			"Output suffix fastq for read2 (the part of the read after identified barcodes)", 2, false, false, false);
	
	
	private static final Collection<ConfigFileOption> optionsRunParam() {
		Collection<ConfigFileOption> rtrn = new ArrayList<ConfigFileOption>();
		rtrn.add(optionOutputPrefix);
		rtrn.add(optionOutputSuffixFastqUnpaired);
		rtrn.add(optionOutputSuffixFastq1);
		rtrn.add(optionOutputSuffixFastq2);
		return rtrn;
	}
	
	private static final ConfigFileSection sectionRunParams = new ConfigFileSection("Run_parameters", optionsRunParam(), true);
	
	
	/***************************************************************************************
	 *                                Read elements
	 ***************************************************************************************/
	
	private static final ConfigFileOption optionBarcodeTable = new ConfigFileOption("barcodes_file", 
			"Table of barcodes with format <id>  <seq>. Config file line: <file>  <barcode_set_name>  <max_mismatch>",
			4, false, true, false);
	
	private static final Collection<ConfigFileOption> optionsReadElement() {
		Collection<ConfigFileOption> rtrn = new ArrayList<ConfigFileOption>();
		rtrn.add(optionBarcodeTable);
		return rtrn;
	}
	
	private static final ConfigFileSection sectionUnpairedReadElements = new ConfigFileSection("Unpaired_read_elements", optionsReadElement(), false);
	private static final ConfigFileSection sectionRead1Elements = new ConfigFileSection("Read1_elements", optionsReadElement(), false);
	private static final ConfigFileSection sectionRead2Elements = new ConfigFileSection("Read2_elements", optionsReadElement(), false);
	
	
	/***************************************************************************************
	 *                                Functionality
	 ***************************************************************************************/
	
	private boolean isPaired;
	
	private static final Collection<ConfigFileSection> sections() {
		Collection<ConfigFileSection> sections = new ArrayList<ConfigFileSection>();
		sections.add(sectionRunParams);
		sections.add(sectionFastq);
		sections.add(sectionUnpairedReadElements);
		sections.add(sectionRead1Elements);
		sections.add(sectionRead2Elements);
		return sections;
	}

	public BarcodeAnalysisConfigFile(String file) throws IOException {
		super(sections(), file);
		validatePaired();
	}
	
	public String getSingleFastq() {return getSingleValueString(sectionFastq, optionUnpairedFastq);}
	public String getPairedFastq1() {return getSingleValueString(sectionFastq, optionRead1Fastq);}
	public String getPairedFastq2() {return getSingleValueString(sectionFastq, optionRead2Fastq);}
	public String getOutputPrefix() {return getSingleValueString(sectionRunParams, optionOutputPrefix);}
	public int getUnpairedReadLength() {return getSingleValueInt(sectionFastq, optionUnpairedReadLength);}
	public int getRead1Length() {return getSingleValueInt(sectionFastq, optionRead1Length);}
	public int getRead2Length() {return getSingleValueInt(sectionFastq, optionRead2Length);}
	public String getOutputSuffixFastqUnpaired() {return getSingleValueString(sectionRunParams, optionOutputSuffixFastqUnpaired);}
	public String getOutputSuffixFastq1() {return getSingleValueString(sectionRunParams, optionOutputSuffixFastq1);}
	public String getOutputSuffixFastq2() {return getSingleValueString(sectionRunParams, optionOutputSuffixFastq2);}
	
	private BarcodedReadLayout getReadLayout(ConfigFileSection section, int readLength) {
		List<OptionValuePair> optionValues = getOrderedOptionsAndValues(section);
		ArrayList<ReadSequenceElement> eltsSequence = new ArrayList<ReadSequenceElement>();
		for(OptionValuePair optionValue : optionValues) {
			if(!optionValue.option().equals(optionBarcodeTable)) {
				throw new IllegalArgumentException("Currently, only support is for table of barcodes");
			}
			ConfigFileOptionValue value = optionValue.value();
			String barcodeTable = value.asString(1);
			String name = value.asString(2);
			int maxMismatch = value.asInt(3);
			try {
				Collection<FragmentBarcode> barcodes = FragmentBarcode.createBarcodesFromTable(barcodeTable, maxMismatch);
				eltsSequence.add(new BarcodeSet(name, barcodes));
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(-1);
			}
		}
		return new BarcodedReadLayout(eltsSequence, readLength);
	}
	
	private void validatePaired() {
		if(hasSection(sectionRead1Elements) && hasSection(sectionRead2Elements)
				&& hasOption(sectionFastq, optionRead1Length) && hasOption(sectionFastq, optionRead2Length)) {
			if(hasSection(sectionUnpairedReadElements)) {
				exitWithMessageAndHelpMenu("Provide both paired fastq sections or unpaired fastq section");
			}
			isPaired = true;
		} else if(hasSection(sectionUnpairedReadElements) && hasOption(sectionFastq, optionUnpairedReadLength)) isPaired = false;
		else exitWithMessageAndHelpMenu("Provide both paired fastq sections and both lengths, or unpaired fastq section and length");
	}
	
	public boolean isPaired() {return isPaired;}
	
	public BarcodedReadLayout getUnpairedReadLayout() {return getReadLayout(sectionUnpairedReadElements, getSingleValueInt(sectionFastq, optionUnpairedReadLength));}
	public BarcodedReadLayout getRead1Layout() {return getReadLayout(sectionRead1Elements, getSingleValueInt(sectionFastq, optionRead1Length));}
	public BarcodedReadLayout getRead2Layout() {return getReadLayout(sectionRead2Elements, getSingleValueInt(sectionFastq, optionRead2Length));}
	
	
	
	
	
	
	
}
