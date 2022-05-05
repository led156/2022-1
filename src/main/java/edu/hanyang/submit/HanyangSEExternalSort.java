package edu.hanyang.submit;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import io.github.hyerica_bdml.indexer.ExternalSort;
import org.apache.commons.lang3.tuple.MutableTriple;


public class HanyangSEExternalSort implements ExternalSort {

    /**
     * External sorting     
     * @param infile    Input file
     * @param outfile   Output file
     * @param tmpdir    Temporary directory to be used for writing intermediate runs on 
     * @param blocksize Available blocksize in the main memory of the current system
     * @param nblocks   Available block numbers in the main memory of the current system
     * @throws IOException  Exception while performing external sort
     */
    @Override
    public void sort(String infile, String outfile, String tmpdir, int blocksize, int nblocks) throws IOException {
        // 1) initial phase
    	ArrayList<MutableTriple<Integer, Integer, Integer>> dataArr = new ArrayList<>(nElement);
    	//...
    	
    	/// 2) n-way merge
    	_externalMergeSort(tmpdir, outfile, 0);    	
    }
    
    private void _externalMergeSort(String tmpDir, String outputFile, int step) throws IOException {
    	File[] fileArr = (new File(tmpDir + File.separator + String.valueOf(prevStep))).listFiles();
    	if (fileArr.length <= nblocks - 1) {
    		for (File f : fileArr) {
    			DataInputStream dos = new ... (f.getAbsolutePath(), blocksize);
    		}
    	}
    	else {
    		for (File f : fileArr) {
    			// ...
    			cnt++;
    			if (cnt == nblocks - 1) {
    				//n_way_merge());
    			}
    		}
    		_externalMergeSort(tmpDir, outputFile, step+1);
    	}
    }
    
    public void n_way_merge(List<DataInputStream> files, String outputFile) throws IOException {
    	PriorityQueue<DataManager> queue = new PriorityQueue<>(files.size(), new Comparator<DataManager>() {
    																			public int compare(DataManager o1, DataManager o2) {
    																				return o1.tuple.compareTo(o2.tuple);
    																			}
    																		});
    	
    	while (queue.size() != 0) {
    		DataManager dm = queue.poll();
    		MutableTriple<Integer, Integer, Integer> tmp = dm.getTuple();
    		//...
    	}
    }
}

class DataManager {

	protected Object tuple;

	public MutableTriple<Integer, Integer, Integer> getTuple() {
		// TODO Auto-generated method stub
		return null;
	}
	
}

