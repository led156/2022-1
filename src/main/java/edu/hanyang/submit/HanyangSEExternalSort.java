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
	String prevStep;
	int step;
	String tmpdir;
	int nFile = 0;
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
    	this.tmpdir = tmpdir;
        int nElement = nblocks*blocksize / (3*4);
        
		// 1) initial phase : nElement 만큼의 tuple 을 저장할 ArrayList 생성.
        ArrayList<MutableTriple<Integer, Integer, Integer>> dataArr = new ArrayList<>(nElement);
		
    	DataInputStream is = new DataInputStream(
     		   new BufferedInputStream(
     				   new FileInputStream(infile), blocksize));
        int data = -1;
        
        System.out.println("start");
        Files.createDirectories(Paths.get(tmpdir + String.valueOf("initial") + File.separator));
        // dataArr 에 데이터 추가. nElement 만큼 읽고, 어레이가 다 차거나/다 읽으면 tmpdir(initial) 를 통해 해당 어레이 파일화
       
        
    	while( is.available() > 0 ) {
    		int tmpLeft = 0;
            int tmpMiddle = 0;
            int tmpRight = 0;
         	int cnt = 1;
         	while(dataArr.size() < nElement && is.available() > 0 && (data = is.readInt()) != -1) { /*array is not full*/
         		// (a) add array
         		if(cnt%3 == 1) {
         			//tmpT.setLeft(data);
         			tmpLeft = data;
            	}
            	else if(cnt%3 == 2) {
            		//tmpT.setMiddle(data);
            		tmpMiddle = data;
            	}
            	else {
            		//tmpT.setRight(data);
            		tmpRight = data;
            		dataArr.add(new MutableTriple<Integer, Integer, Integer>(tmpLeft, tmpMiddle, tmpRight));	// 삼항 모두 완료.. 어레이에 넣음.
         		}
         		cnt++;
         	}
         	// (b) array -> out file
         	System.out.println("one initial array create");
        	DataOutputStream os = new DataOutputStream(
          		   new BufferedOutputStream(
          				 new FileOutputStream(tmpdir + String.valueOf("initial") + File.separator + String.valueOf(nFile) + String.valueOf(".data"))));
         	// (i) sort
        	System.out.println("start sort");
         	Collections.sort(dataArr);
         	System.out.println("end sort");
         	// (ii) write
         	for(MutableTriple<Integer, Integer, Integer> m : dataArr) {
         		os.writeInt(m.getLeft());
         		os.writeInt(m.getMiddle());
         		os.writeInt(m.getRight());
         		os.flush();
         	}
         	dataArr.clear();
         	os.close();
         	nFile++;
         }
         is.close();
        
    	/// 2) n-way merge
        System.out.println("_externalMergeSort(tmpdir, outfile, 0);");
    	_externalMergeSort(tmpdir, outfile, 0);    	
    }
    
    private void _externalMergeSort(String tmpDir, String outputFile, int step) throws IOException {
    	this.step = step;
    	prevStep = (step == 0) ? "initial" : String.valueOf(step - 1);
    	List<DataInputStream> files = new ArrayList<DataInputStream>();
    	nFile = 0;
    	
    	
    	// 파일 리스트 저장.
    	File[] fileArr = (new File(tmpDir + File.separator + String.valueOf(prevStep))).listFiles();
    	
    	// 총 파일 수가 nblocks - 1 ( = N) 이하일 때 : 
    	if (fileArr.length <= nblocks - 1) {	// N-way 수를 넘지 않을때.
    		for (File f : fileArr) {
    			// 파일 읽기..
    			DataInputStream dos = new DataInputStream(
    		     		   new BufferedInputStream(
    		     				   new FileInputStream(f.getAbsolutePath()), blocksize));
    			// List<DataInputStream> 만들기.
    			files.add(dos);
    		}
    		// ...
    		
    		n_way_merge(files, outputFile);
    		System.out.println("n_way_merge" + "in EMS"+step+" length");
    	}
    	
    	
    	// 총 파일 수가 nblocks - 1 을 넘을 때 -> 여러번 비교.
    	else {
    		int cnt = 0;
    		for (File f : fileArr) {
    			Files.createDirectories(Paths.get(tmpdir + String.valueOf(step) + File.separator));
    			// 파일 읽기..
    			DataInputStream dos = new DataInputStream(
    		     		   new BufferedInputStream(
    		     				   new FileInputStream(f.getAbsolutePath()), blocksize));
    			// List<DataInputStream> 만들
    			files.add(dos);
    			cnt++;
    			if (cnt == nblocks - 1) {	//cnt 가 n-way에 도달했다면 merge.
    				String tmpOutputFile = tmpdir + String.valueOf(step) + File.separator + String.valueOf(nFile) + String.valueOf(".data");
    				n_way_merge(files, tmpOutputFile);
    				nFile++;
    				System.out.println("n_way_merge" + "in EMS"+step);
    				cnt = 0;
    			}
    		}
    		System.out.println("_externalMergeSort(tmpdir, outfile, "+ (step+1) +");");
    		_externalMergeSort(tmpDir, outputFile, step+1);
    	}
    }
    
    public void n_way_merge(List<DataInputStream> files, String outputFile) throws IOException {
    	PriorityQueue<DataManager> queue = new PriorityQueue<>(files.size(), new Comparator<DataManager>() {
    																			public int compare(DataManager o1, DataManager o2) {
    																				return o1.tuple.compareTo(o2.tuple);
    																			}
    																		});
    	
    	// queue에 DataManager 넣기.
    	for(DataInputStream is : files) {
    		DataManager dm = new DataManager(is);
        	queue.add(dm);
    	}
    	
    	
    	DataOutputStream os = new DataOutputStream(
	      		   new BufferedOutputStream(
	      				 new FileOutputStream(outputFile)));
    	
    	// 정렬된 queue에서 퉤
    	while (queue.size() != 0) {
    		DataManager dm = queue.poll();
    		MutableTriple<Integer, Integer, Integer> tmp = dm.getTuple();
    		//... 순서대로 tuple 나옴.이 tuple 을 결과에 넣기 
    		os.writeInt(tmp.getLeft());
     		os.writeInt(tmp.getMiddle());
     		os.writeInt(tmp.getRight());
     		os.flush();
     		if (!dm.isEOF) {queue.add(dm);}
    	}
    	os.close();
    }
}

class DataManager {	// dis를 받아 그..관리하는. tuple은 지금 커서가 가리키고 있는 튜플을 뜻함.
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

