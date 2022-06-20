package edu.hanyang.submit;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import io.github.hyerica_bdml.indexer.ExternalSort;
import org.apache.commons.lang3.tuple.MutableTriple;


public class HanyangSEExternalSort implements ExternalSort {
	int nblocks;
	int blocksize;
	
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
    	this.nblocks = nblocks;
    	this.blocksize = blocksize;
    	
    	int nFile = 0;
    	//int nElement = blocksize * nblocks / (24);	// R's size = nElement * 12
    	int nElement = blocksize * nblocks / (4*3);
    	
		// 1) initial phase
        ArrayList<MutableTriple<Integer, Integer, Integer>> dataArr = new ArrayList<>(nElement);
    	DataInputStream is = new DataInputStream(
     		   new BufferedInputStream(
     				   new FileInputStream(infile), blocksize));
        
        Files.createDirectories(Paths.get(tmpdir + String.valueOf("initial") + File.separator));
        
        int NofInt = is.available() / 4;
        int nRelation = NofInt / (nElement*3);
        int nRemain = (NofInt % (nElement*3))/3;
        
        //System.out.println("is size = "+is.available()+"\tNofInt = "+NofInt+"\tnRelation = "+nRelation+"\tnRemain = "+nRemain);
        
        
        // make arrayList & sort -> make initial run
        for (int i = 0; i < nRelation; i++) {
        	// (a) add array
        	for (int j = 0; j < nElement; j++) {
        		dataArr.add(new MutableTriple<Integer, Integer, Integer>(is.readInt(), is.readInt(), is.readInt()));
        	}
        	
     		// (b) array -> out file
        	DataOutputStream os = new DataOutputStream(
           		   new BufferedOutputStream(
           				 new FileOutputStream(tmpdir + "initial" + File.separator + String.valueOf(nFile) + ".data")));
         	// (i) sort
          	Collections.sort(dataArr);
          	
          	// (ii) write
          	for(MutableTriple<Integer, Integer, Integer> m : dataArr) {
          		os.writeInt(m.getLeft());
          		os.writeInt(m.getMiddle());
          		os.writeInt(m.getRight());
          		os.flush();
          	}
          	os.close();
          	dataArr = new ArrayList<MutableTriple<Integer, Integer, Integer>>(nElement);
          	nFile++;
        }
        
        if (nRemain > 0 && is.available() > 0) {
        	// (a) add array
        	for (int j = 0; (j < nRemain) ; j++) {
            	dataArr.add(new MutableTriple<Integer, Integer, Integer>(is.readInt(), is.readInt(), is.readInt()));
            }
        	
        	// (b) array -> out file
        	DataOutputStream os = new DataOutputStream(
           		   new BufferedOutputStream(
           				 new FileOutputStream(tmpdir + "initial" + File.separator + String.valueOf(nFile) + ".data")));
         	// (i) sort
          	Collections.sort(dataArr);
          	// (ii) write
          	for(MutableTriple<Integer, Integer, Integer> m : dataArr) {
          		os.writeInt(m.getLeft());
          		os.writeInt(m.getMiddle());
          		os.writeInt(m.getRight());
          		os.flush();
          	}
          	os.close();
          	dataArr = null;
          	nFile++;
        }
        
 		is.close();
 		
    	/// 2) n-way merge
    	_externalMergeSort(tmpdir, outfile, 0);
    }
    
    private void _externalMergeSort(String tmpDir, String outputFile, int step) throws IOException {
    	String prevStep = (step == 0) ? "initial" : String.valueOf(step - 1);
    	List<DataInputStream> files = new ArrayList<DataInputStream>(nblocks-1);
    	int nFile = 0;
    	
    	// fileArr : store file list
    	File[] fileArr = (new File(tmpDir + File.separator + prevStep)).listFiles();
    	
    	// if file# <= nblocks-1 (=N)
    	if (fileArr.length <= nblocks - 1) {	
    		for (File f : fileArr) {
    			// make inputStream
    			DataInputStream dos = new DataInputStream(
    		     		   new BufferedInputStream(
    		     				   new FileInputStream(f.getAbsolutePath()), blocksize));
    			// add to List<DataInputStream>
    			files.add(dos);
    		}
    		// ...
    		n_way_merge(files, outputFile);
    	}
    	
    	// if file# > nblocks-1 (=N) -> iterate n-way merge
    	else {
    		int cnt = 0;
    		Files.createDirectories(Paths.get(tmpDir + String.valueOf(step) + File.separator));
    		for (File f : fileArr) {
    			// make inputStream
    			DataInputStream dos = new DataInputStream(
    		     		   new BufferedInputStream(
    		     				   new FileInputStream(f.getAbsolutePath()), blocksize));
    			// add to List<DataInputStream>
    			files.add(dos);
    			cnt++;
    			
    			
    			if (cnt >= nblocks - 1) {	// each cnt = n -> merge
    				n_way_merge(files, tmpDir + String.valueOf(step) + File.separator + String.valueOf(nFile) + ".data");
    				nFile++;
    				cnt = 0;
    				files = new ArrayList<DataInputStream>(nblocks-1);
    			}
    		}
    		
    		if(files.size() > 0) {
    			n_way_merge(files, tmpDir + String.valueOf(step) + File.separator + String.valueOf(nFile) + ".data");
				nFile++;
				cnt = 0;
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
    	
    	// add DataManager to queue
    	for(DataInputStream is : files) {
        	queue.add(new DataManager(is));
    	}
    	
    	DataOutputStream os = new DataOutputStream(
	      		   new BufferedOutputStream(
	      				 new FileOutputStream(outputFile)));
    	
    	// get DataManager by sequential
    	while (queue.size() != 0) {
    		DataManager dm = queue.poll();
    		MutableTriple<Integer, Integer, Integer> tmp = dm.getTuple();
    		os.writeInt(tmp.getLeft());
     		os.writeInt(tmp.getMiddle());
     		os.writeInt(tmp.getRight());
     		os.flush();
     		if (!dm.isEOF) {queue.add(dm);}
    	}
    	os.close();
    }
}

class DataManager {
	public boolean isEOF = false;
	private DataInputStream dis = null;
	public MutableTriple<Integer, Integer, Integer> tuple = new MutableTriple<Integer, Integer, Integer>(0, 0, 0);
	
	public DataManager(DataInputStream dis) throws IOException {
		this.dis = dis;
		readNext();
	}
	
	private boolean readNext() throws IOException {
		if (isEOF) return false;
		try {
			tuple.setLeft(dis.readInt()); 
			tuple.setMiddle(dis.readInt()); 
			tuple.setRight(dis.readInt());
			return true;
		} catch (EOFException e) {
			return false;
		}
	}

	public MutableTriple<Integer, Integer, Integer> getTuple() throws IOException{
		MutableTriple<Integer, Integer, Integer> ret = new MutableTriple<Integer, Integer, Integer>(0, 0, 0);
		ret.setLeft(tuple.getLeft());
		ret.setMiddle(tuple.getMiddle()); 
		ret.setRight(tuple.getRight());
		isEOF = (! readNext());
		return ret;
	}
	
}
