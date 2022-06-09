package edu.hanyang.submit;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;

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
	
	public void printTree() throws IOException {
		int filelength = (int) raf.length();
		mraf.seek(0); 
		System.out.println(mraf.readInt());
		for (int i = 0; i < filelength/serializeSize; i++) {
			Block block = loadBlock(i*serializeSize);
			System.out.println(i*serializeSize + "type:"+block.type + "nkeys:"+block.nkeys + "pBlock:"+block.parent+"\t" 
					+Arrays.toString(block.keys)+Arrays.toString(block.vals)+"\t");
		}
		System.out.println();
	}
	
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
        
        if (mraf.length() <= 0 || raf.length() <= 0) {
        	Block root = new Block(rootindex, maxKeys);
        	saveBlock(root);
        	saveRidx(root.my_pos);
        }
        else {
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
			e.printStackTrace();
			return null;
		}
    }
    
    public void saveBlock(Block block) {
    	try {
    		byte[] arrays = ser.serialize(block);
    		raf.seek(block.my_pos);
    		raf.write(arrays);
    	} catch (IOException e) {
			e.printStackTrace();
		}
    }
    
    public void saveRidx(int idx) {
    	try {
    		this.rootindex = idx;
    		mraf.seek(0);
        	mraf.writeInt(idx);
    	} catch (IOException e) {
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
        Block block = searchNode(key);	// search leaf node
        if (block.nkeys + 1 > maxKeys) {	// if block has maximum Keys
        	Block newBlock = split(block, key, value);		// i) split block
        	saveBlock(block);
        	saveBlock(newBlock);
        	// 2) insert new node to parent node
        	if (block.my_pos == rootindex) {	// if block is root node -> no parent node
        		// new parent node of block
        		Block pBlock = new Block((int) raf.length(), maxKeys, 1);
        		pBlock.setKeys(0, newBlock.keys[0]);	// insert key
        		pBlock.setVals(0, block.my_pos);	// insert pointer of block, newBlock
        		pBlock.setVals(1, newBlock.my_pos);
        		pBlock.setNkeys(1);
        		block.parent = pBlock.my_pos;
        		newBlock.parent = pBlock.my_pos;
        		saveBlock(block);
        		saveBlock(newBlock);
        		saveBlock(pBlock);
        		saveRidx(pBlock.my_pos);
        	}
        	else {	// block has parent node
        		newBlock.parent = block.parent;
        		saveBlock(newBlock);
        		insertInternal(block.parent, newBlock.keys[0], newBlock.my_pos);
        	}
        }
        else {	// block has space
        	int i = 0;
        	if (block.nkeys == 0) {
        		block.insertKeys(i, key, value);
        		saveBlock(block);
        	}
        	else {
        		for (i = 0; i < block.nkeys; i++) {
            		if (block.keys[i] > key) {
            			break;
            		}
            	}
        		block.insertKeys(i, key, value);
            	saveBlock(block);
        	}
        }
    }
    
    private void insertInternal(int parent, int key, int vals) throws IOException {
    	Block pBlock = loadBlock(parent);
    	
		if (pBlock.nkeys + 1 > maxKeys) {	// if pBlock has maximum Keys
			Block newBlock = new Block((int) raf.length(), maxKeys, 1);
			int middle = split(pBlock, newBlock, key, vals);	// 1) split block
			saveBlock(pBlock);
			saveBlock(newBlock);
			// 2) insert new node to parent node of pBlock
			if (pBlock.my_pos == rootindex) {	// if pBlock is root node -> no parent node
				// new parent node of pBlock
				Block ppBlock = new Block((int) raf.length(), maxKeys, 1);
				ppBlock.setKeys(0, middle);	// insert key
				ppBlock.setVals(0, pBlock.my_pos);	// insert pointer of pBlock, newBlock
				ppBlock.setVals(1, newBlock.my_pos);
				ppBlock.setNkeys(1);
        		pBlock.parent = ppBlock.my_pos;
        		newBlock.parent = ppBlock.my_pos;
        		saveBlock(pBlock);
        		saveBlock(newBlock);
        		saveBlock(ppBlock);
        		saveRidx(ppBlock.my_pos);
			}
			else {	// pBlock has parent node
				newBlock.parent = pBlock.parent;
        		saveBlock(newBlock);
				insertInternal(pBlock.parent, middle, newBlock.my_pos);
			}
		}
		else {	// pBlock has space
			int i = 0;
			for (i = 0; i < pBlock.nkeys; i++) {
        		if (pBlock.keys[i] > key) {
        			break;
        		}
        	}
			pBlock.insertNodes(i, key, vals);
			saveBlock(pBlock);
		}
    }
	
	
	private Block split(Block block, int key, int val) throws IOException {	// split leaf node
		Block newBlock = new Block((int) raf.length(), maxKeys);
		int n = (int) Math.ceil(maxKeys/2+1);
		
		int[] keylist = Arrays.copyOf(block.keys, maxKeys + 1);
		keylist[maxKeys] = key;
		Arrays.sort(keylist);
		
		int idx = ArrayUtils.indexOf(keylist, key);
		
		int[] vallist = Arrays.copyOf(block.vals, maxKeys + 2);
		for (int i = maxKeys+1; i > idx; i--) {
			vallist[i] = vallist[i-1];
		}
		vallist[idx] = val;
		newBlock.setVals(maxKeys, block.vals[maxKeys]);
		block.setVals(maxKeys, newBlock.my_pos);
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
		
		for (int i = 0; i < maxKeys; i++) {
			if (i < n) {
				block.setVals(i, vallist[i]);
				if (i == n-1 && maxKeys%2 == 0) {
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
		block.setNkeys(n);
		newBlock.setNkeys(maxKeys + 1 - n);
		
		return newBlock;
	}
	
	
	private int split(Block lBlock, Block rBlock, int key, int val) {	// // split non-leaf node
		int middleKey = 0;
		int n = (int) Math.ceil((double)(maxKeys+1)/2);
		
		int[] keylist = Arrays.copyOf(lBlock.keys, maxKeys + 1);
		keylist[maxKeys] = key;
    	Arrays.sort(keylist);
    	
    	int idx = ArrayUtils.indexOf(keylist, key);
    	
    	int[] vallist = Arrays.copyOf(lBlock.vals, maxKeys + 2);
    	for (int i = maxKeys+1; i > idx+1; i--) {
			vallist[i] = vallist[i-1];
		}
		vallist[idx+1] = val;
		
		if (maxKeys%2 == 1) { // maxKeys is odd -> N is even
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
				if (i < n+1) {
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
		else {	// maxKeys is even -> N is odd
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
		
		for (int i = 0; i < rBlock.nkeys+1; i++) {
			Block eBlock = loadBlock(rBlock.vals[i]);
			eBlock.parent = rBlock.my_pos;
			saveBlock(eBlock);
		}
		
		return middleKey;
	}
	
	
	private Block searchNode(int key) {	// Returns a leaf node that has the potential for the key to be located.
    	// start to root node
    	Block block = loadBlock(rootindex);
    	
    	// search until block is leaf node
    	while (block.type == 1) {
    		int cnt = 0;
    		for (int i = 0; i < block.nkeys; i++) {
    			if (block.keys[i] <= key) {
    				cnt++;
    			}
    		}
    		block = loadBlock(block.vals[cnt]);
    	}
		return block;
	}

	/**
     * B+ tree에 있는 데이터를 탐색하는 함수
     * @param key 탐색할 key
     * @return 탐색된 value 값
     * @throws IOException
     */
    @Override
    public int search(int key) throws IOException {
        Block rb = loadBlock(rootindex);
        return _search(rb, key);
    }

	public int _search(Block block, int key) throws IOException {
		int val = -1;
		
		// search until block is leaf node
		while (block.type == 1) {
			int cnt = 0;
			for (int i = 0; i < block.nkeys; i++) {
				if (block.keys[i] == key) {
					cnt = i+1;
					break;
				}
				else if (block.keys[i] > key) {
					cnt = i;
					break;
				}
				if (i == block.nkeys - 1) {
					cnt = block.nkeys;
					break;
				}
    		}
			block = loadBlock(block.vals[cnt]);
		}
		
		for (int i = 0; i < block.nkeys; i++) {
			if (block.keys[i] == key) {	
    			val = block.vals[i];
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
    	raf.close();
    	mraf.close();
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


class Block implements Serializable {
	private static final long serialVersionUID = 1L;
	public int type;	// leaf : 0, non-leaf : 1
	public int my_pos;	// position of block node
	public int parent;	// parent position of block node
	public int nkeys;		// number of keys currently present
	public int maxKeys;		// number of possible keys 
	public int[] vals;		// leaf : value, non-leaf : pointer
    public int[] keys;
    
    public Block(int maxKeys) {	// constructor node
    	vals = new int[maxKeys+1];
    	keys = new int[maxKeys];
    }

	public Block(int pos, int maxKeys) {	// constructor leaf node
    	this.type = 0;
    	this.my_pos = pos;
    	this.nkeys = 0;
    	this.maxKeys = maxKeys;
    	vals = new int[maxKeys+1];
    	keys = new int[maxKeys];
    }
    
    public Block(int pos, int maxKeys, int type) {	// constructor non-leaf node(leaf node)
    	this.type = type;
    	this.my_pos = pos;
    	this.nkeys = 0;
    	this.maxKeys = maxKeys;
    	vals = new int[maxKeys+1];
    	keys = new int[maxKeys];
    }
    
    public void insertKeys(int idx, int key, int value) {	// only leaf nodes are available. insert key & value in idx
    	for (int i = nkeys; i > idx; i--) {
    		keys[i] = keys[i-1];
    		vals[i] = vals[i-1];
    	}
    	keys[idx] = key;
    	vals[idx] = value;
    	nkeys++;
    }
    
    public void insertNodes(int idx, int key, int value) {	// only non-leaf nodes are available. insert key & value in idx
    	for (int i = nkeys; i > idx; i--) {
    		keys[i] = keys[i-1];
    		vals[i+1] = vals[i];
    	}
    	keys[idx] = key;
    	vals[idx+1] = value;
    	nkeys++;
	}
    
    public void setKeys(int idx, int key) {
    	this.keys[idx] = key;
    }
    
    public void setVals(int idx, int val) {
    	this.vals[idx] = val;
    }
	
    public void setNkeys(int n) {
    	this.nkeys = n;
    }
}
