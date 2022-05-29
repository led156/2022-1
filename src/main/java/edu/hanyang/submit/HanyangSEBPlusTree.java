package edu.hanyang.submit;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.lang.instrument.Instrumentation;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;

import io.github.hyerica_bdml.indexer.BPlusTree;



public class HanyangSEBPlusTree implements BPlusTree {
	int blocksize;
	int nblocks;
	byte[] buf;
	ByteBuffer buffer;
	int maxKeys;
	RandomAccessFile raf;
	Serializer ser;
	int serializeSize;
	String treefile;
	
	int rootindex = 0;
	
    /**
     * B+ tree를 open하는 함수(파일을 열고 준비하는 단계 구현)
     * @param metafile B+ tree의 메타정보 저장(저장할거 없으면 안써도 됨)
     * @param treefile B+ tree의 메인 데이터 저장
     * @param blocksize B+ tree 작업 처리에 이용할 데이터 블록 사이즈
     * @param nblocks B+ tree 작업 처리에 이용할 데이터 블록 개수
     * @throws IOException
     */
    @Override
    public void open(String metafile, String treefile, int blocksize, int nblocks) throws IOException {
        this.blocksize = blocksize;
        this.nblocks = nblocks;
        this.buf = new byte[blocksize];
        this.buffer = ByteBuffer.wrap(buf);
        this.maxKeys = (blocksize - 16) / 8;
        this.treefile = treefile;
        
        raf = new RandomAccessFile(treefile, "rw");
        ser = new Serializer();
    	
        serializeSize = (ser.serialize(new Block(maxKeys))).length;
        
        // (i) meta 파일 읽고 rootindex 읽어오기.
        
        
    	// (ii) root 만들기. or 불러오기. - buffer.
        if (raf.length() > 0) {	// 불러오
        	loadBlock(rootindex);
        }
        else {
        	Block root = new Block(rootindex, maxKeys);
        	saveBlock(root);
        }
    }
    
    public Block loadBlock(int pos) {
		byte[] arrays = new byte[serializeSize];
		try {
			raf.seek(pos);
			raf.read(arrays);
			return ser.deserialize(arrays);
		} catch (IOException | ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
    }
    
    public void saveBlock(Block block) throws IOException {
		byte[] arrays = ser.serialize(block);
		raf.seek(block.my_pos);
		raf.write(arrays);
    }

    /**
     * B+ tree에 데이터를 삽입하는 함수
     * @param key
     * @param value
     * @throws IOException
     */
    @Override
    public void insert(int key, int value) throws IOException {
        Block block = searchNode(key);	// 해당 key가 들어갈만한 노드 블럭을 찾아온다. 이제 block은 무족권 leaf 노드~.
        if (block.nkeys + 1 > maxKeys) {	// 블럭이 꽉 찬 상태. 
        	Block newBlock = split(block, key, value);		// 1) 블럭을 n/2 만큼 나눠준다.
        	// 2) 새로운 노드를 패런츠에 insert
        	if (block.my_pos == rootindex) {	// block이 root라면 -> 부모 노드가 없으면.
        		// newBlock의 제일 앞 키를 따오고
        		// vals[0] 과 vals[1]을 block, newBlock의 포인터로 지정한다.
        	}
        	else {
        		insertInternal(block.parent, newBlock.keys[0], newBlock.my_pos);
        	}
        }
        else {	// 블럭이 꽉 차지 않은 상태.
        	// leaf 블럭에 해당 밸류를 추가한다.
        	// nkeys 만큼 순회하고,	
        	int i = 0;
        	if (block.nkeys == 0) {
        		block.insertKeys(i, key, value);
        	}
        	else {
        		for (i = 0; i < block.nkeys; i++) {
            		if (block.keys[i] > key) {
            			break;
            		}
            	}
            	block.insertKeys(i, key, value);
        	}
        	
        	
        }
    }
    
    private void insertInternal(int parent, int key, int vals) {
    	Block pBlock = loadBlock(parent);
    	
		// parent가 꽉 찬 노드
		if (pBlock.nkeys + 1 > maxKeys) {
			// split
			Block newBlock = null;
			int middle = split(pBlock, newBlock, key, vals);
			if (pBlock.my_pos == rootindex) {	// 만약 parent node가 root
				// key를 넣고.
	    		// vals[0] 과 vals[1]을 block, newBlock의 포인터로 지정한다.
			}
			else {	// 아니다 패런트 노드는 부모 노드가 있다.
				// insertInternal
				insertInternal(pBlock.parent, middle, newBlock.my_pos);
			}
		}
		
		// parent에 여유 있음.
		else {
			// parent node에 key를 추가.
			// parent node keys 재정렬.
			// key가 들어간 Idx를 찾아 block 포인터를 집어넣음.
			int i = 0;
			for (i = 0; i < pBlock.nkeys; i++) {
        		if (pBlock.keys[i] > key) {
        			break;
        		}
        	}
			pBlock.insertKeys(i, key, vals);
		}
    }

	private Block searchNode(int key) {	// search 는 키가 가진 밸류를 찾아오지만, 해당 메소드는 키가 있는 노드(블럭)을 찾아온다. // 해당 key가 들어갈만한 노드 블럭을 찾아온다.
    	// root 노드부터 탐색.
    	Block block = loadBlock(rootindex);
    	int type = block.type;
    	
    	// non-leaf가 아닐때까지 이를 반복하여 결국 리프 노드를 뱉어내야함. // non-leaf 일 동안 반복.
		// i) 돌고 돌아 첫번째키가 해당 포인트보다 작다면 포인터를 따라간다.
    	while (type == 1) {
    		// nkeys 만큼 순회하고,	
        	for (int i = 0; i < block.nkeys; i++) {
        		// 만약 keys[i] > key 라면
        		if (block.keys[i] > key) {
        			block = loadBlock(block.vals[i]);
        			type = block.type;
        			break;
        		}
        	}
    	}
		return block;
	}
	
	
	private Block split(Block block, int key, int val) throws IOException {	// 블럭을 split 해주고 새로 생긴 블럭을 리턴하는 메소드. leaf인 노드임.
		Block newBlock = new Block((int) raf.length(), maxKeys);
		
		// block의 key와 새로운 key를 합쳐 정렬, list에 b의 key를 저장.
		int[] list = Arrays.copyOf(block.keys, maxKeys + 1);
    	// newK를 넣고,
    	list[maxKeys] = key;
    	// 해당 List를 정렬.
    	Arrays.sort(list);
    	int idx = Arrays.asList(list).indexOf(key);
    	
    	
    	int[] listt = Arrays.copyOf(block.vals, maxKeys + 2);
    	for (int i = maxKeys+1; i > idx; i++) {
    		listt[i] = listt[i-1];
    	}
    	listt[idx] = val;
    	
		int n = (int) Math.ceil(maxKeys/2);
		
		int[] keyList = new int[maxKeys];
		keyList = Arrays.copyOfRange(list, 0, n);
		// 정렬된 List를 n/2 만큼 originalB. -> 덮어쓰
		block.setKeys(keyList);
		keyList = Arrays.copyOfRange(list, n, list.length);
		// 나머지 n/2 를 newB. -> 덮어쓰기
		newBlock.setKeys(keyList);
		
		int[] valList = new int[maxKeys+1];
		valList = Arrays.copyOfRange(listt, 0, n);
		block.setVals(valList);
		valList = Arrays.copyOfRange(listt, n, listt.length);
		newBlock.setVals(valList);
		
		return newBlock;
	}
	
	
	
	// (i) split
	// (1) parent와 newBlock의 제일 앞 키를 따오고
	// (2) parent node keys 재정렬
	// (3) newBlock의 키값이 들어간 Idx를 찾아 newBlock의 포인터값을 vals에 집어넣음.
	// (4) 재정렬된 keys를 Left, Middle, Right로 나눈다. (포인터까지 나눠지기 때문에, 다시 생각할필요X)
	private int split(Block lBlock, Block rBlock, int key, int val) {	// 블럭을 split해주고 중간값을 리턴해주는 메소드.
		int middleKey;
		
		// (1)
		// block의 key와 새로운 key를 합쳐 정렬, list에 b의 key를 저장.
		int[] list = Arrays.copyOf(lBlock.keys, maxKeys + 1);
    	// newK를 넣고,
    	list[maxKeys] = key;
    	// (2)
    	// 해당 List를 정렬.
    	Arrays.sort(list);
    	// (3)
    	int idx = Arrays.asList(list).indexOf(key);
    	
    	
    	int[] listt = Arrays.copyOf(lBlock.vals, maxKeys + 2);
    	for (int i = maxKeys+1; i > idx; i++) {
    		listt[i] = listt[i-1];
    	}
    	listt[idx] = val;
    	
    	// (4)
    	int n = (int) Math.ceil(maxKeys/2);
    	if (maxKeys%2 == 0) {
    		middleKey = list[n];
    		// lB 의 key 는 0~n-1까지, vals 는 0~n까지
    		// rB 의 key 는 n+1~maxKeys, vals 는 n+1~maxKeys+1까지
    		int[] keyList = new int[maxKeys];
    		keyList = Arrays.copyOfRange(list, 0, n);
    		lBlock.setKeys(keyList);
    		keyList = Arrays.copyOfRange(list, n+1, list.length);
    		rBlock.setKeys(keyList);
    		
    		int[] valList = new int[maxKeys+1];
    		valList = Arrays.copyOfRange(listt, 0, n+1);
    		lBlock.setVals(valList);
    		valList = Arrays.copyOfRange(listt, n+1, listt.length);
    		rBlock.setVals(valList);
    	}
    	else {
    		middleKey = list[n-1];
    		// lB 의 key 는 0~n-2까지, vals 는 0~n-1까지
    		// rB 의 key 는 n~maxKeys, vals 는 n~maxKeys+1까지
    		int[] keyList = new int[maxKeys];
    		keyList = Arrays.copyOfRange(list, 0, n-1);
    		lBlock.setKeys(keyList);
    		keyList = Arrays.copyOfRange(list, n, list.length);
    		rBlock.setKeys(keyList);
    		
    		int[] valList = new int[maxKeys+1];
    		valList = Arrays.copyOfRange(listt, 0, n);
    		lBlock.setVals(valList);
    		valList = Arrays.copyOfRange(listt, n, listt.length);
    		rBlock.setVals(valList);
    	}
    	return middleKey;
	}
	
	private Block readBlock(int pointer) {	// pointer를 통해 해당 블럭을 읽어오는 메소드.
		return loadBlock(pointer);
	}

	/**
     * B+ tree에 있는 데이터를 탐색하는 함수
     * @param key 탐색할 key
     * @return 탐색된 value 값
     * @throws IOException
     */
    @Override
    public int search(int key) throws IOException {
        Block rb = readBlock(rootindex);	// 루트 블럭을 가져온다.
        return _search(rb, key);
    }
    
    

	public int _search(Block b, int key) throws IOException {
		int val = -1;
    	if (b.type == 1) {	// non-leaf
    		// ...
    		Block child = null;
    		for (int i = 0; i < b.nkeys; i++) {	// 블럭을 하나씩 탐색.
    			if (b.keys[i] >= key) {		// 만약 블럭의 해당 키 값이 찾고있는 키 값보다 작다면, child 블럭으로 감.
        			child = readBlock(b.vals[i]);
        			break;
        		}
    		}
    		val = _search(child, key);
    	}
    	else {	// leaf
    		/* binary or linear search */
    		for (int i = 0; i < b.nkeys; i++) {	// 블럭을 하나씩 탐색.
    			if (b.keys[i] == key) {		// 
        			val = b.vals[i];
        			break;
        		}
    		}
    	}
    	return val;
    }

    /**
     * B+ tree를 닫는 함수, 열린 파일을 닫고 저장하는 단계
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        // TODO: your code here...
    	raf.close();
    }
    
    
    public static void main(String[] args) throws IOException, ClassNotFoundException {
    	System.out.println(Integer.SIZE*(5+(10*2)));
    	RandomAccessFile r = new RandomAccessFile("serial.data", "rw");
		Serializer ser = new Serializer();
    	System.out.println(r.length());
    	System.out.println(r.getFilePointer());
    	Block block = new Block((int) r.length(), 10);
    	block.keys[0] = 3412;
    	block.keys[1] = 343;
    	block.keys[6] = 343;
    	Block block2 = new Block(10);
    	block2.parent = 1;
    	
    	byte[] write = ser.serialize(block2);
    	r.write(write);
    	System.out.println(r.getFilePointer());
    	System.out.println(r.length());
    	System.out.println(write.length);
    	System.out.println("-------");
    	System.out.println((ser.serialize(new Block(10))).length);
		r.close();
    }
}

class Serializer {
	public byte[] serialize(Block obj) throws IOException {
	    try(ByteArrayOutputStream b = new ByteArrayOutputStream()){
	        try(ObjectOutputStream o = new ObjectOutputStream(b)){
	            o.writeObject(obj);
	        }
	        return b.toByteArray();
	    }
	}
	
	public Block deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
	    try(ByteArrayInputStream b = new ByteArrayInputStream(bytes)){
	        try(ObjectInputStream o = new ObjectInputStream(b)){
	            return (Block) o.readObject();
	        }
	    }
	}
}


// 노드 블럭을 나타내는 클래스.
class Block implements Serializable {
	private static final long serialVersionUID = 1L;
	public int type;	// 해당 블럭의 leaf, non-leaf를 나타냄. 1이면 non-leaf.
	public int my_pos;	// 해당 블럭의 위치를 나타내는 값.
	public int parent;	// 해당 블럭의 부모의 위치를 나타내는 값.
	public int nkeys;		// 해당 블럭에 몇개의 키가 차있는지.
	public int maxKeys;
	public int[] vals;		// non-leaf 에선 pointer 역할을 하고, leaf에선 value를 담는 일을 한다.
    public int[] keys;
    
    public Block(int maxKeys) {
    	vals = new int[maxKeys+1];
    	keys = new int[maxKeys];
    }
    
    public Block(int pos, int maxKeys) {	// 새로운 노드를 만드는 행
    	this.type = 0;
    	this.my_pos = pos;
    	this.nkeys = 0;
    	this.maxKeys = maxKeys;
    	vals = new int[maxKeys+1];
    	keys = new int[maxKeys];
    }
    
    public void insertKeys(int idx, int key, int value) {
    	if (this.type != 1) {
    		vals[nkeys+1] = vals[nkeys];
    	}
    	
    	for (int i = nkeys; i > idx; i--) {
    		keys[i] = keys[i-1];
    		vals[i] = keys[i-1];
    	}
    	keys[idx] = key;
    	vals[idx] = value;
    	nkeys++;
    }
    
    public void setKeys(int[] list) {
		this.keys = list;
	}
    
    public void setVals(int[] list) {
		this.vals = list;
	}
	
}
