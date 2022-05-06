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
import java.nio.file.Path;
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
        int nElement = nblocks;
        
        
		// 1) initial phase : nElement 만큼의 tuple 을 저장할 ArrayList 생성.
    	ArrayList<MutableTriple<Integer, Integer, Integer>> dataArr = new ArrayList<>(nElement);
    		
    	DataInputStream is = new DataInputStream(
     		   new BufferedInputStream(
     				   new FileInputStream(infile), blocksize));
        DataOutputStream os = new DataOutputStream(
     		   new BufferedOutputStream(
     				   new FileOutputStream(tmpdir + File.separator + "initial.DATA"), blocksize));
        int cnt;
        int data = -1;
        MutableTriple<Integer, Integer, Integer> tmpT = new MutableTriple<>();
        // dataArr 에 데이터 추가. nElement 만큼 읽고, 어레이가 다 차거나/다 읽으면 tmpdir(initial) 를 통해 해당 어레이 파일화
        while( is.available() > 0 ) {
        	cnt = 1;
        	while(dataArr.size() < nElement && (data = is.readInt()) != -1) { /*array is not full*/
        		// (a) add array
        		if(cnt%3 == 1) {
            		tmpT.setLeft(data);
            		cnt++;
            	}
            	else if(cnt%3 == 2) {
            		tmpT.setMiddle(data);
            		cnt++;
            	}
            	else {
            		tmpT.setRight(data);
            		dataArr.add(tmpT);	// 삼항 모두 완료.. 어레이에 넣음.
            		cnt++;
            	}
        	}
        	// (b) array -> out file
        	// (i) sort
        	Collections.sort(dataArr);
        	// (ii) write
        	for(MutableTriple<Integer, Integer, Integer> m : dataArr) {
        		tmpT.setLeft(m.getLeft());
        		tmpT.setMiddle(m.getMiddle());
        		tmpT.setRight(m.getRight());
        		try {
					os.writeBytes(tmpT.toString());
				} catch (IOException e) {
					e.printStackTrace();
				}
        	}
        	dataArr.removeAll(dataArr);
        }
        
        is.close();
        os.close();
    	
    	/// 2) n-way merge
    	_externalMergeSort(tmpdir, outfile, 0);    	
    }
    
    private void _externalMergeSort(String tmpDir, String outputFile, int step) throws IOException {
    	String prevStep = (step == 0) ? "initial" : String.valueOf(step - 1);
    	List<DataInputStream> files = new ArrayList<DataInputStream>();
    	
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
    	}
    	
    	
    	// 총 파일 수가 nblocks - 1 을 넘을 때 -> 여러번 비교.
    	else {
    		int cnt = 0;
    		for (File f : fileArr) {
    			// 파일 읽기..
    			DataInputStream dos = new DataInputStream(
    		     		   new BufferedInputStream(
    		     				   new FileInputStream(f.getAbsolutePath()), blocksize));
    			// List<DataInputStream> 만들
    			files.add(dos);
    			cnt++;
    			if (cnt == nblocks - 1) {	//cnt 가 n-way에 도달했다면 merge.
    				n_way_merge(files, outputFile);
    				cnt = 0;
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
    	
    	// queue에 DataManager 넣기.
    	for(DataInputStream is : files) {
    		DataManager dm = new DataManager(is);
        	queue.add(dm);
    	}
    	
    	// 정렬된 queue에서 퉤
    	while (queue.size() != 0) {
    		DataManager dm = queue.poll();
    		MutableTriple<Integer, Integer, Integer> tmp = dm.getTuple();
    		//... 순서대로 tuple 나옴.이 tuple 을 결과에 넣기 
    	}
    }
    
    
    
    
    public static void main(String[] args) throws IOException {
    	String INPUT_DATA_PATH = "data/input_10000000.data";
        String OUTPUT_DATA_PATH = "data/output_10000000.data";
        String TEMP_DIR_PATH = "tmp/";
        int BLOCKSIZE = 1024;
        int N_BLOCKS = 1000;
        
//        ExternalSort sort;
//        sort = new HanyangSEExternalSort();
        
        if (Files.exists(Paths.get(TEMP_DIR_PATH)) && Files.isDirectory(Paths.get(TEMP_DIR_PATH))) {
            removeDir(Paths.get(TEMP_DIR_PATH));
        } else {
            Files.createDirectory(Paths.get(TEMP_DIR_PATH));
        }
        
        int nElement = 3;
        
        
        ArrayList<MutableTriple<Integer, Integer, Integer>> dataArr = new ArrayList<>(nElement);
		
    	DataInputStream is = new DataInputStream(
     		   new BufferedInputStream(
     				   new FileInputStream(INPUT_DATA_PATH), BLOCKSIZE));
        DataOutputStream os = new DataOutputStream(
     		   new BufferedOutputStream(
     				   new FileOutputStream(TEMP_DIR_PATH + "test.txt"), BLOCKSIZE));
        
        // dataArr 에 데이터 추가. nElement 만큼 읽고, 어레이가 다 차거나/다 읽으면 tmpdir(initial) 를 통해 해당 어레이 파일화
        DataManager dm = new DataManager(is);
        
        while (!dm.isEOF) {
        	// (a) add array item
        	while (dataArr.size() < nElement && !dm.isEOF) {
        		dataArr.add(dm.getTuple());
        	}
        	
        	// (b) array -> out file
        		// (i) sort
        	// ...
        		// (ii) write
        	for (MutableTriple<Integer, Integer, Integer> m : dataArr) {
        		os.writeBytes(m.toString());
        	}
        	dataArr.clear();
        }
        is.close();
        os.close();
        System.out.println("complete all");
    }
    
    private static void removeDir(final Path path) throws IOException {
        if (Files.exists(path) && Files.isDirectory(path)) {
            for (File file : path.toFile().listFiles()) {
                if (file.isDirectory())
                    removeDir(file.toPath());
                else
                    Files.deleteIfExists(file.toPath());
            }
        }
    }
}

class DataManager {	// dis를 받아 그..관리하는. tuple은 지금 커서가 가리키고 있는 튜플을 뜻함.
	public boolean isEOF = false;
	private DataInputStream dis = null;
	public MutableTriple<Integer, Integer, Integer> tuple = new MutableTriple<Integer, Integer, Integer>(0, 0, 0);
	
	public DataManager(DataInputStream dis) throws IOException {
		this.dis = dis;
		this.tuple = this.getTuple();
	}
	
	private boolean readNext() throws IOException {
		try {
			if (isEOF) return false;
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

