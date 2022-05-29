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

import org.apache.commons.lang3.ArrayUtils;

import io.github.hyerica_bdml.indexer.BPlusTree;



public class HanyangSEBPlusTree implements BPlusTree {
	int blocksize;
	int nblocks;
	byte[] buf;
	ByteBuffer buffer;
	int maxKeys;
	RandomAccessFile raf;
	RandomAccessFile mraf;
	Serializer ser;
	int serializeSize;
	String treefile;
	String metafile;
	
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
        this.metafile = metafile;
        mraf = new RandomAccessFile(metafile, "rw");
        raf = new RandomAccessFile(treefile, "rw");
        ser = new Serializer();
    	
        serializeSize = (ser.serialize(new Block(maxKeys))).length;
        
        
        
    	// (ii) root 만들기. or 불러오기. - buffer.
        if (raf.length() == 0) {
        	Block root = new Block(rootindex, maxKeys);
        	saveBlock(root);
        }
        else {
        	// (i) meta 파일 읽고 rootindex 읽어오기.
        	mraf.seek(0);
        	rootindex = mraf.readInt();
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
    
    public void saveBlock(Block block) {
    	try {
    		byte[] arrays = ser.serialize(block);
    		int length = (int) raf.length();
    		raf.seek(block.my_pos);
    		raf.write(arrays);
    		System.out.println("arrays size: "+arrays.length+"write size: "+(raf.length()- length));
    	} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    public void saveRidx(int idx) {
    	try {
    		this.rootindex = idx;
    		mraf.seek(0);
        	mraf.writeInt(idx);
    	} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
        	saveBlock(block);
        	saveBlock(newBlock);
        	// 2) 새로운 노드를 패런츠에 insert
        	if (block.my_pos == rootindex) {	// block이 root라면 -> 부모 노드가 없으면.
        		// newBlock의 제일 앞 키를 따오고
        		// vals[0] 과 vals[1]을 block, newBlock의 포인터로 지정한다.
        		System.out.println("raf.length()"+raf.length());
        		Block pBlock = new Block((int) raf.length(), maxKeys, 1);
        		pBlock.setKeys(0, newBlock.keys[0]);
        		pBlock.setVals(0, block.my_pos);
        		pBlock.setVals(1, newBlock.my_pos);
        		pBlock.setNkeys(1);
        		block.parent = pBlock.my_pos;
        		newBlock.parent = pBlock.my_pos;
        		saveBlock(block);
        		saveBlock(newBlock);
        		saveBlock(pBlock);
        		System.out.println(Arrays.toString(block.keys) + Arrays.toString(pBlock.keys) + Arrays.toString(newBlock.keys));
        		System.out.println(Arrays.toString(block.vals) + Arrays.toString(pBlock.vals) + Arrays.toString(newBlock.vals));
        		//rootindex = pBlock.my_pos;
        		saveRidx(pBlock.my_pos);
        	}
        	else {
        		newBlock.parent = block.parent;
        		saveBlock(newBlock);
        		insertInternal(block.parent, newBlock.keys[0], newBlock.my_pos);
        	}
        }
        else {	// 블럭이 꽉 차지 않은 상태.
        	// leaf 블럭에 해당 밸류를 추가한다.
        	// nkeys 만큼 순회하고,	
        	int i = 0;
        	if (block.nkeys == 0) {
        		block.insertKeys(i, key, value);
        		System.out.println(Arrays.toString(block.keys));
        		saveBlock(block);
        	}
        	else {
        		for (i = 0; i < block.nkeys; i++) {
            		if (block.keys[i] > key) {
            			break;
            		}
            	}
            	block.insertKeys(i, key, value);
            	System.out.println(Arrays.toString(block.keys));
            	saveBlock(block);
        	}
        }
    }
    
    private void insertInternal(int parent, int key, int vals) throws IOException {
    	Block pBlock = loadBlock(parent);
    	
		// parent가 꽉 찬 노드
		if (pBlock.nkeys + 1 > maxKeys) {
			// split
			Block newBlock = new Block((int) raf.length(), maxKeys, 1);
			int middle = split(pBlock, newBlock, key, vals);
			saveBlock(pBlock);
			saveBlock(newBlock);
			if (pBlock.my_pos == rootindex) {	// 만약 parent node가 root
				// key를 넣고.
	    		// vals[0] 과 vals[1]을 block, newBlock의 포인터로 지정한다.
				Block ppBlock = new Block((int) raf.length(), maxKeys, 1);
				ppBlock.setKeys(0, middle);
				ppBlock.setVals(0, pBlock.my_pos);
				ppBlock.setVals(1, newBlock.my_pos);
				ppBlock.setNkeys(1);
        		pBlock.parent = ppBlock.my_pos;
        		newBlock.parent = ppBlock.my_pos;
        		saveBlock(pBlock);
        		saveBlock(newBlock);
        		saveBlock(ppBlock);
        		System.out.println(Arrays.toString(pBlock.keys) + pBlock.nkeys + Arrays.toString(ppBlock.keys) + ppBlock.nkeys + Arrays.toString(newBlock.keys)+ newBlock.nkeys );
        		System.out.println(Arrays.toString(pBlock.vals) + pBlock.nkeys + Arrays.toString(ppBlock.vals) + ppBlock.nkeys + Arrays.toString(newBlock.vals)+ newBlock.nkeys );
        		//rootindex = ppBlock.my_pos;
        		saveRidx(ppBlock.my_pos);
			}
			else {	// 아니다 패런트 노드는 부모 노드가 있다.
				// insertInternal
				newBlock.parent = pBlock.parent;
        		saveBlock(newBlock);
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
			pBlock.insertNodes(i, key, vals);
			saveBlock(pBlock);
			System.out.println("pb"+Arrays.toString(pBlock.keys));
			System.out.println("pbv"+Arrays.toString(pBlock.vals));
		}
    }

	private Block searchNode(int key) {	// search 는 키가 가진 밸류를 찾아오지만, 해당 메소드는 키가 있는 노드(블럭)을 찾아온다. // 해당 key가 들어갈만한 노드 블럭을 찾아온다.
    	// root 노드부터 탐색.
    	Block block = loadBlock(rootindex);
    	System.out.println("rootindex"+rootindex);
    	
    	// non-leaf가 아닐때까지 이를 반복하여 결국 리프 노드를 뱉어내야함. // non-leaf 일 동안 반복.
		// i) 돌고 돌아 첫번째키가 해당 포인트보다 작다면 포인터를 따라간다.
    	while (block.type == 1) {
    		int i;
    		// nkeys 만큼 순회하고,	
        	for (i = 0; i < block.nkeys; i++) {
        		// 만약 keys[i] > key 라면
        		if (block.keys[i] > key) {
        			System.out.println("serachNode - load : "+i);
        			block = loadBlock(block.vals[i]);
        			break;
        		}
        	}
        	if (i == block.nkeys) {
        		block = loadBlock(block.vals[i]);
        	}
    	}
		return block;
	}
	
	
	private Block split(Block block, int key, int val) throws IOException {	// 블럭을 split 해주고 새로 생긴 블럭을 리턴하는 메소드. leaf인 노드임.
		Block newBlock = new Block((int) raf.length(), maxKeys);
		int n = (int) Math.ceil(maxKeys/2+1);
		
		int[] keylist = Arrays.copyOf(block.keys, maxKeys + 1);
		keylist[maxKeys] = key;
		
		Arrays.sort(keylist);
		int idx = ArrayUtils.indexOf(keylist, key);
		System.out.println("list::" + n + ": "+Arrays.toString(keylist));
		
		int[] vallist = Arrays.copyOf(block.vals, maxKeys + 2);
		for (int i = maxKeys+1; i > idx; i--) {
			vallist[i] = vallist[i-1];
		}
		vallist[idx] = val;
		
		
		for (int i = 0; i < maxKeys; i++) {
			if (i < n) {
				block.setKeys(i, keylist[i]);
				if (i == n-1 && maxKeys%2 == 0) {
					newBlock.setKeys(i, 0);
				}
				else {
					newBlock.setKeys(i, keylist[n+i]);
				}
			}
			else {
				block.setKeys(i, 0);
				newBlock.setKeys(i, 0);
			}
		}
		
		for (int i = 0; i < maxKeys+1; i++) {
			if (i < n) {
				block.setVals(i, vallist[i]);
				if (i == n-1 && maxKeys%2 == 1) {
					newBlock.setVals(i, 0);
				}
				else {
					newBlock.setVals(i, vallist[n+i]);
				}
			}
			else {
				block.setVals(i, 0);
				newBlock.setVals(i, 0);
			}
		}
		block.setVals(maxKeys, newBlock.my_pos);
		block.setNkeys(n);
		newBlock.setNkeys(maxKeys + 1 - n);
		
		return newBlock;
	}
	
	
	
	// (i) split
	// (1) parent와 newBlock의 제일 앞 키를 따오고
	// (2) parent node keys 재정렬
	// (3) newBlock의 키값이 들어간 Idx를 찾아 newBlock의 포인터값을 vals에 집어넣음.
	// (4) 재정렬된 keys를 Left, Middle, Right로 나눈다. (포인터까지 나눠지기 때문에, 다시 생각할필요X)
	private int split(Block lBlock, Block rBlock, int key, int val) {	// 블럭을 split해주고 중간값을 리턴해주는 메소드.
		int middleKey = 0;
		int n = (int) Math.ceil(maxKeys/2+1);
	
		// (1)
		// block의 key와 새로운 key를 합쳐 정렬, list에 b의 key를 저장.
		int[] keylist = Arrays.copyOf(lBlock.keys, maxKeys + 1);
    	// newK를 넣고,
		keylist[maxKeys] = key;
    	// (2)
    	// 해당 List를 정렬.
    	Arrays.sort(keylist);
    	// (3)
    	int idx = ArrayUtils.indexOf(keylist, key);
    	System.out.println("list::" + n + ": "+Arrays.toString(keylist));
    	
    	int[] vallist = Arrays.copyOf(lBlock.vals, maxKeys + 2);
    	for (int i = maxKeys+1; i > idx+1; i--) {
			vallist[i] = vallist[i-1];
		}
		vallist[idx+1] = val;
    	
		
		if (maxKeys%2 == 1) { //N은 짝수
			for (int i = 0; i < maxKeys+1; i++) {
				if (i < n) {
					lBlock.setKeys(i, keylist[i]);
					if (i == n-1) {
						rBlock.setKeys(i, 0);
					}
					else {
						rBlock.setKeys(i, keylist[n+1+i]);
					}
				}
				else if (i == n) {
					middleKey = keylist[i];
				}
				else {
					lBlock.setKeys(i-1, 0);
					rBlock.setKeys(i-1, 0);
				}
			}
			
			for (int i = 0; i < maxKeys+1; i++) {
				if (i <= n) {
					lBlock.setVals(i, vallist[i]);
					if (i == n) {
						rBlock.setVals(i, 0);
					}
					else {
						rBlock.setVals(i, vallist[n+1+i]);
					}
				}
				else {
					lBlock.setVals(i, 0);
					rBlock.setVals(i, 0);
				}
			}
			lBlock.setNkeys(n);
			rBlock.setNkeys(maxKeys - n);
		}
		else {
			for (int i = 0; i < maxKeys+1; i++) {
				if (i < n-1) {
					lBlock.setKeys(i, keylist[i]);
					rBlock.setKeys(i, keylist[n+i]);
				}
				else if (i == n-1) {
					middleKey = keylist[i];
				}
				else {
					lBlock.setKeys(i-1, 0);
					rBlock.setKeys(i-1, 0);
				}
			}
			
			for (int i = 0; i < maxKeys+1; i++) {
				if (i < n) {
					lBlock.setVals(i, vallist[i]);
					rBlock.setVals(i, vallist[n+i]);
				}
				else {
					lBlock.setVals(i, 0);
					rBlock.setVals(i, 0);
				}
			}
			lBlock.setNkeys(n - 1);
			rBlock.setNkeys(maxKeys - (n - 1)); 
		}
		
		return middleKey;
	}
	

	/**
     * B+ tree에 있는 데이터를 탐색하는 함수
     * @param key 탐색할 key
     * @return 탐색된 value 값
     * @throws IOException
     */
    @Override
    public int search(int key) throws IOException {
        Block rb = loadBlock(rootindex);	// 루트 블럭을 가져온다.
        return _search(rb, key);
    }

	public int _search(Block b, int key) throws IOException {
		int val = -1;
		
		while (b.type == 1) {
			int cnt = 0;
			for (int i = 0; i < b.nkeys; i++) {	// 블럭을 하나씩 탐색.
    			if (b.keys[i] <= key) {		
    				cnt++;
        		}
    		}
			System.out.println("Search in : " + Arrays.toString(b.keys) + cnt);
			b = loadBlock(b.vals[cnt]);
		}
		
		System.out.println("Search in leaf : " + Arrays.toString(b.keys));
		for (int i = 0; i < b.nkeys; i++) {	// 블럭을 하나씩 탐색.
			if (b.keys[i] == key) {		// 
    			val = b.vals[i];
    			break;
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
//    	mraf.seek(0);
//    	mraf.writeInt(rootindex);
    	mraf.close();
    }
    
    
    public static void main(String[] args) throws IOException, ClassNotFoundException {
		HanyangSEBPlusTree tree = new HanyangSEBPlusTree();
		tree.open("sdf.meta", "try.tree", 52, 10);
 
		tree.insert(5, 10);
		tree.insert(6, 15);
		tree.insert(4, 20);
		tree.insert(7, 1);
		tree.insert(8, 5);
		tree.insert(17, 7);
		tree.insert(30, 8);
		tree.insert(1, 8);
		
		tree.insert(58, 1);
		tree.insert(25, 8);
		tree.insert(96, 32);
		tree.insert(21, 8);
		tree.insert(9, 98);
		
		tree.insert(57, 54);
		tree.insert(157, 54);
		tree.insert(247, 54);
		tree.insert(357, 254);
		tree.insert(557, 54);
		
		tree.close();
		
		tree = new HanyangSEBPlusTree();
		tree.open("sdf.meta", "try.tree", 52, 10);
 
		// Check search function
		System.out.println(tree.search(357));
		System.out.println(tree.search(557));
		System.out.println(tree.search(4));
//		assertEquals(tree.search(7), 1);
//		assertEquals(tree.search(8), 5);
//		assertEquals(tree.search(17), 7);
//		assertEquals(tree.search(30), 8);
//		assertEquals(tree.search(1), 8);
//		assertEquals(tree.search(58), 1);
//		assertEquals(tree.search(25), 8);
//		assertEquals(tree.search(96), 32);
//		assertEquals(tree.search(21), 8);
//		assertEquals(tree.search(9), 98);
//		assertEquals(tree.search(57), 54);
//		assertEquals(tree.search(157), 54);
//		assertEquals(tree.search(247), 54);
//		assertEquals(tree.search(357), 254);
//		assertEquals(tree.search(557), 54);
		
		
		tree.close();
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
    
    public void insertNodes(int idx, int key, int value) {
    	for (int i = nkeys; i > idx; i--) {
    		keys[i] = keys[i-1];
    		vals[i+1] = vals[i];
    	}
    	keys[idx] = key;
    	vals[idx+1] = value;
    	nkeys++;
	}

	public Block(int pos, int maxKeys) {	// 새로운 노드를 만드는 행
    	this.type = 0;
    	this.my_pos = pos;
    	this.nkeys = 0;
    	this.maxKeys = maxKeys;
    	vals = new int[maxKeys+1];
    	keys = new int[maxKeys];
    }
    
    public Block(int pos, int maxKeys, int type) {	// 새로운 노드를 만드는 행, non-leaf
    	this.type = type;
    	this.my_pos = pos;
    	this.nkeys = 0;
    	this.maxKeys = maxKeys;
    	vals = new int[maxKeys+1];
    	keys = new int[maxKeys];
    }
    
    public void insertKeys(int idx, int key, int value) {
    	if (this.type == 1) {	// non - leaf
    		vals[nkeys+1] = vals[nkeys];
    	}
    	
    	for (int i = nkeys; i > idx; i--) {
    		keys[i] = keys[i-1];
    		vals[i] = vals[i-1];
    	}
    	keys[idx] = key;
    	vals[idx] = value;
    	nkeys++;
    }
    
    public void setKeys(int[] list) {
		this.keys = list;
	}
    
    public void setKeys(int idx, int key) {
    	this.keys[idx] = key;
    }
    
    public void setVals(int[] list) {
		this.vals = list;
	}
    
    public void setVals(int idx, int val) {
    	this.vals[idx] = val;
    }
	
    public void setNkeys(int n) {
    	this.nkeys = n;
    }
}
