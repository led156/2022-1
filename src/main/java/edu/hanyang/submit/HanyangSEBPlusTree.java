package edu.hanyang.submit;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
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
        	Block newnode = split(block, key, value);		// 1) 블럭을 n/2 만큼 나눠준다.
        	insertInternal(block.parent, newnode.my_pos);	// 2) 새로운 노드를 패런츠에 insert
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

	private void insertInternal(int parent, int my_pos) {
		Block block = new Block(my_pos, treefile, maxKeys);
		
		// if parent가 null 이라면
		if (parent == -1) {
			// 새로운 Parent블럭 생성.
			Block pBlock = new Block(maxKeys);
			// my_pos로 받는 자식 노드의 앞 부분을 떼와 자신의 첫 키값으로 할당.
			pBlock.keys[0] = block.keys[0];
			// 첫 포인터를 어쩌구..
			pBlock.vals[0] = ;
			// 두번째 포인터를 my_pos로 지정.
			pBlock.vals[1] = my_pos;
		}
			
		
		// else parent가 null 이 아니라
		else {
			Block pBlock = new Block(parent, treefile, maxKeys);
			
			// if 패런트 블럭의 포인터가 다 찼다면.. ?
			if (pBlock.nkeys + 1 > maxKeys) {
				// 받은 my_pos의 앞 키값과 자신의 키값을 합쳐 정렬 -> split
				block = split(pBlock, block.keys[0], my_pos);
				// 나머지 포인터들 재연결
				//	block에 대해 insertExternal
			}
	    	
	    	// 포인터가 다 차지 않았다면
			else {
				
				int i = 0;
				int key = block.keys[0];
				for (i = 0; i < pBlock.nkeys; i++) {
            		if (pBlock.keys[i] > pBlock.keys[0]) {
            			break;
            		}
            	}
				
				pBlock.insertKeys(i, key, my_pos);
			}
		}
	}

	private Block split(Block block, int key, int val) {	// 블럭을 split 해주고 새로 생긴 블럭을 리턴하는 메소드.
		Block newBlock = new Block(maxKeys);
		
		// block의 key와 새로운 key를 합쳐 정렬, list에 b의 key를 저장.
		int[] list = Arrays.copyOf(block.keys, maxKeys + 1);
    	// newK를 넣고,
    	list[maxKeys] = key;
    	// 해당 List를 정렬.
    	Arrays.sort(list);
    	int idx = Arrays.asList(list).indexOf(key);
		
    	
    	
		
		int n = (int) Math.ceil(maxKeys/2);
		if (idx < n) {	// in original
			
		}
		else { // in new
			
		}
		// 정렬된 List를 n/2 만큼 originalB. -> 덮어쓰
		block.setKeys(oKeyList);
		// 나머지 n/2 를 newB. -> 덮어쓰기
		newBlock.setKeys(nKeyList);
		
		return newBlock;
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
    
    
    public static void main(String[] args) throws IOException {
    	
    	String metapath = "./tmp/bplustree.meta";
		String savepath = "./tmp/bplustree.tree";
		int blocksize = 52;
		int nblocks = 10;
 
		File treefile = new File(savepath);
		if (treefile.exists()) {
			if (! treefile.delete()) {
				System.err.println("error: cannot remove files");
				System.exit(1);
			}
		}

		HanyangSEBPlusTree tree = new HanyangSEBPlusTree();
		tree.open(metapath, savepath, blocksize, nblocks);
 
		tree.insert(5, 10);
		System.out.println(tree.search(5));
    	
    	
    }
}

// 노드 블럭을 나타내는 클래스.
class Block {
	public int type;	// 해당 블럭의 leaf, non-leaf를 나타냄. 1이면 non-leaf.
	public int my_pos;	// 해당 블럭의 위치를 나타내는 값.
	public int parent;	// 해당 블럭의 부모의 위치를 나타내는 값.
	public int nkeys;		// 해당 블럭에 몇개의 키가 차있는지.
	public int maxKeys;
	public int[] vals;		// non-leaf 에선 pointer 역할을 하고, leaf에선 value를 담는 일을 한다.
    public int[] keys;
    
    RandomAccessFile raf = null;
    
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
