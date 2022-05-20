package edu.hanyang.submit;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import io.github.hyerica_bdml.indexer.BPlusTree;
import scala.reflect.internal.Trees.Block;


public class HanyangSEBPlusTree implements BPlusTree {
	int blocksize;
	int nblocks;
	byte[] buf;
	ByteBuffer buffer;
	int maxKeys;
	RandomAccessFile raf;
	
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
        Block block = searchNode(key);
        
        if (block.nkeys + 1 > maxKeys) {
        	Block newnode = split(block, key, val);
        	insertInternal(block.parent, newnode.my_pos);
        }
        else {
        	// ...
        }
    }

    /**
     * B+ tree에 있는 데이터를 탐색하는 함수
     * @param key 탐색할 key
     * @return 탐색된 value 값
     * @throws IOException
     */
    @Override
    public int search(int key) throws IOException {
        Block rb = readBlock(rootindex);
        return _search(rb, key);
    }
    
    public int _search(Block b, int key) throws IOException {
    	if (b.type == 1) {
    		// ...
    		if (block.keys[i] < key) {
    			child = readBlock(b.vals[i]);
    		}
    		// ..
    	}
    	else {
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
