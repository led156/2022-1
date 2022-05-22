package edu.hanyang.submit;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import io.github.hyerica_bdml.indexer.BPlusTree;


public class HanyangSEBPlusTree implements BPlusTree {
	int blocksize;
	int nblocks;
	byte[] buf;
	ByteBuffer buffer;
	int maxKeys;
	RandomAccessFile raf;
	
	int rootindex;
	
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
        Block block = searchNode(key);	// 해당 밸류가 있는 leaf 블럭의 인덱스를 찾아 해당 블럭을 받아옴.
        
        if (block.nkeys + 1 > maxKeys) {	// 블럭이 꽉 찬 상태. 
        	Block newnode = split(block, key, val);		// 1) 블럭을 n/2 만큼 나눠준다.
        	insertInternal(block.parent, newnode.my_pos);	// 2) 패런츠에 insert
        }
        else {	// 블럭이 꽉 차지 않은 상태.
        	// ...
        	// leaf 블럭에 해당 밸류를 추가한다.
        }
    }

    private Block searchNode(int key) {	// search 는 키가 가진 밸류를 찾아오지만, 해당 메소드는 키가 있는 노드(블럭)을 찾아온다.
		// TODO Auto-generated method stub
		return null;
	}

	private void insertInternal(Object parent, int my_pos) {
		// TODO Auto-generated method stub
		// if 패런트 블럭의 포인터가 다 찼다면.. ?
    	
    	// 포인터가 다 차지 않았다면
	}

	private Block split(Block block, int key, int val) {	// 블럭을 split 해주고 새로 생긴 블럭을 리턴하는 메소드.
		// TODO Auto-generated method stub
		return null;
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
    
    private Block readBlock(int rootindex2) {
		// TODO Auto-generated method stub
		return null;
	}

	public int _search(Block b, int key) throws IOException {
    	if (b.type == 1) {	// non-leaf
    		// ...
    		for (int i = 0; i < ; i++) {	// 블럭을 하나씩 탐색.
    			if (block.keys[i] < key) {		// 만약 블럭의 해당 키 값이 찾고있는 키 값보다 작다면, child 블럭으로 감.
        			child = readBlock(b.vals[i]);
        			break;
        		}
    		}
    		_search(child, key);
    		
    		// ..
    	}
    	else {	// leaf
    		/* binary or linear search */
    		// if exists,
    		return val;
    		// else
    		return -1;
    	}
    }

    /**
     * B+ tree를 닫는 함수, 열린 파일을 닫고 저장하는 단계
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        // TODO: your code here...
    }
}

class Block {
	public Object[] vals;
	public int type;
	public int my_pos;
	public Object parent;
	public int nkeys;
	
}
