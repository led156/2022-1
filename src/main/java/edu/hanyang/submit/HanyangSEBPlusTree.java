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
        System.out.println(block.nkeys);
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
    			block.save();
        	}
        	else {
        		for (i = 0; i < block.nkeys; i++) {
            		if (block.keys[i] > key) {
            			break;
            		}
            	}
            	block.insertKeys(i, key, value);
    			block.save();
        	}
        	
        	
        }
    }
    
    private void insertInternal(int parent, int key, int vals) {
    	Block pBlock = new Block(parent, treefile, maxKeys);
    	
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
    	Block block = new Block(rootindex, treefile, maxKeys);
    	int type = block.type;
    	
    	// non-leaf가 아닐때까지 이를 반복하여 결국 리프 노드를 뱉어내야함. // non-leaf 일 동안 반복.
		// i) 돌고 돌아 첫번째키가 해당 포인트보다 작다면 포인터를 따라간다.
    	while (type == 1) {
    		// nkeys 만큼 순회하고,	
        	for (int i = 0; i < block.nkeys; i++) {
        		// 만약 keys[i] > key 라면
        		if (block.keys[i] > key) {
        			block = new Block(block.vals[i], treefile, maxKeys);
        			type = block.type;
        			break;
        		}
        	}
    	}
		return block;
	}
	
	
	private Block split(Block block, int key, int val) {	// 블럭을 split 해주고 새로 생긴 블럭을 리턴하는 메소드. leaf인 노드임.
		Block newBlock = new Block(maxKeys);
		
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
		return new Block(pointer, treefile, maxKeys);
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
    }
    
    
    public static void main(String[] args) throws IOException, ClassNotFoundException {
    	
//    	String metapath = "./tmp/bplustree.meta";
//		String savepath = "./tmp/bplustree.tree";
//		int blocksize = 52;
//		int nblocks = 10;
// 
//		File treefile = new File(savepath);
//		if (treefile.exists()) {
//			if (! treefile.delete()) {
//				System.err.println("error: cannot remove files");
//				System.exit(1);
//			}
//		}
//
//		HanyangSEBPlusTree tree = new HanyangSEBPlusTree();
//		tree.open(metapath, savepath, blocksize, nblocks);
// 
//		tree.insert(5, 10);
//		System.out.println(tree.search(5));
    	
    	
		Foo test1 = new Foo(4, 5);
		Foo test2 = new Foo(4);
		
		Block block = new Block(0, "dfdfdf.txt", 10);
		
		RandomAccessFile r = new RandomAccessFile("serial.data", "rw");
		Serializer ser = new Serializer();
		
//		System.out.println(r.getFilePointer());
//		byte[] arrays = ser.serialize(test1);
//		r.write(arrays);
//		System.out.println(r.getFilePointer());
//		arrays = ser.serialize(test2);
//		r.write(arrays);
//		System.out.println(r.getFilePointer());
//		
//		byte[] readd = new byte[arrays.length];
//		r.seek(0);
//		r.read(readd);
//		
//		Foo result = (Foo) ser.deserialize(readd);
//		
//		System.out.println("num :" + result.num);
//		System.out.println("num2 :" + result.num2);
		
		System.out.println(r.getFilePointer());
		byte[] arrays = ser.serialize(block);
		r.write(arrays);
		System.out.println(r.getFilePointer());
		
		byte[] readd = new byte[arrays.length];
		r.seek(0);
		r.read(readd);
		
		Block readB = (Block) ser.deserialize(readd);
		
		System.out.println("readB maxKeys : " + readB.maxKeys);
		
		
		
		r.close();
    }
}

class Foo implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * 
	 */
	public int num;
	public int num2;
	public int[] arrays;
	
	public Foo(int num) {
		this.num = num;
		arrays = new int[num];
	}
	
	public Foo(int num, int num2) {
		this.num = num;
		arrays = new int[num];
		this.num2 = num2;
		for (int i = 0; i < arrays.length; i++) {
			arrays[i] = num2;
		}
	}
}


class Serializer {

	public byte[] serialize(Object obj) throws IOException {
	    try(ByteArrayOutputStream b = new ByteArrayOutputStream()){
	        try(ObjectOutputStream o = new ObjectOutputStream(b)){
	            o.writeObject(obj);
	        }
	        return b.toByteArray();
	    }
	}
	
	public Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
	    try(ByteArrayInputStream b = new ByteArrayInputStream(bytes)){
	        try(ObjectInputStream o = new ObjectInputStream(b)){
	            return o.readObject();
	        }
	    }
	}
}


// 노드 블럭을 나타내는 클래스.
class Block implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public int type;	// 해당 블럭의 leaf, non-leaf를 나타냄. 1이면 non-leaf.
	public int my_pos;	// 해당 블럭의 위치를 나타내는 값.
	public int parent;	// 해당 블럭의 부모의 위치를 나타내는 값.
	public int nkeys;		// 해당 블럭에 몇개의 키가 차있는지.
	public int maxKeys;
	public int[] vals;		// non-leaf 에선 pointer 역할을 하고, leaf에선 value를 담는 일을 한다.
    public int[] keys;
    
    transient RandomAccessFile raf = null;
    
    public Block(int maxKeys) {
    	
    }

	public Block(int pos, String treefile, int maxKeys) {
		this.maxKeys = maxKeys;
    	
    	try {
    		this.raf = new RandomAccessFile(treefile, "rw");
			raf.seek(pos);
			// pos 값에 안착!! - 해당 Pos에 아무것도 없다면 새로운 Block을 만들어줌.
	    	if (raf.length() <= 0) {
	    		this.type = 0;
	    		this.my_pos = pos;
	    		this.parent = -1;
	    		this.nkeys = 0;
	    		this.vals = new int[maxKeys + 1];
	    		this.keys = new int[maxKeys];
	    	}
	    	
	    	else {
	    		// pos부터 node size까지 값을 읽는다. 만약 nulㅣ 값이라면, 새로 만들어줌!
	    		this.type = raf.readInt();
	    		this.my_pos = raf.readInt();
	    		this.parent = raf.readInt();
	    		this.nkeys = raf.readInt();
	    		this.vals = new int[maxKeys + 1];
	    		this.keys = new int[maxKeys];
	    		int i;
	    		for (i = 0; i < nkeys; i++) {
	    			vals[i] = raf.readInt();
	    			keys[i] = raf.readInt();
	    		}
	    		vals[i] = raf.readInt();
	    		
	    		if (nkeys != maxKeys && type == 1) {	// leaf 노드일때 마지막 포인터와 다음 노드를 이어주는것.
	    			raf.seek(my_pos+Integer.BYTES*(4+2*maxKeys));
	    			vals[maxKeys] = raf.readInt();
	    		}
	    	}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
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
    
    public void save() {
    	try {
			raf.seek(my_pos);
			raf.writeInt(type);
	    	raf.writeInt(my_pos);
	    	raf.writeInt(parent);
	    	raf.writeInt(nkeys);
	    	
	    	for (int i = 0; i < maxKeys; i++) {
	    		raf.writeInt(vals[i]);
	    		raf.writeInt(keys[i]);
			}
	    	raf.writeInt(vals[maxKeys]);
	    	
	    	raf.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
	
}
