/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb { 

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++) 
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

  int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}

/*
 * This destructor flushes out all dirty pages, deallocates bufPool and bufDescTable.
 */
BufMgr::~BufMgr() {
    for (std::uint32_t i = 0; i < numBufs; i++) {
        if(bufDescTable[i].dirty) {
            //TODO: implement flushFile
            //flushFile(bufDescTable[i].file);
        }
    }

    delete [] bufDescTable;
    delete [] bufPool;
}

void BufMgr::advanceClock()
{
    clockHand = (clockHand + 1) % (numBufs);
}

void BufMgr::allocBuf(FrameId & frame) 
{
    std::uint32_t scanned= 0; //times of scan executed
    bool found = false; //indicate whether a available buffer is found

    while (scanned <= numBufs) {
        scanned++;
        advanceClock();

        //check if valid set; if not, use this frame
        if (!bufDescTable[clockHand].valid) {
            found = true;
            break;
        }
        else if(bufDescTable[clockHand].refbit) { //if the frame was recently referenced, reset refbit
            bufDescTable[clockHand].refbit = false;
            continue;
        }
        else if(bufDescTable[clockHand].pinCnt > 0 ) { //if the frame is pinned
            continue;
        }
        else { //not pinned, so use this frame; write to disk if dirty
            found = true;
            //remove page from hashtable
            hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
            if(bufDescTable[clockHand].dirty) {
                bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
                bufDescTable[clockHand].dirty = false;
            }
            break;
        }
    }

    //if all buffer frames are pinned
    if (scanned > numBufs && !found) {
        throw BufferExceededException();
    }

    //initialize buffer frame
    bufDescTable[clockHand].Clear();

    frame = clockHand;

}

	
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{

}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{

}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{

}

void BufMgr::flushFile(const File* file) 
{

}

void BufMgr::disposePage(File* file, const PageId PageNo)
{

}

void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
	int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
