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
    FrameId frameNo; //pointer to the frame
    try {
        //find the page
        hashTable->lookup(file, pageNo, frameNo);
        //page is in the buffer pool, so set refbit, increment, pinCnt, and return the pointer
        bufDescTable[frameNo].refbit = true;
        bufDescTable[frameNo].pinCnt++;
        page = &bufPool[frameNo];
    }
    catch (HashNotFoundException e) { //Page is not in the buffer pool.
        //So allocate buffer frame, read the page, insert the page, and invoke Set()
        allocBuf(frameNo);
        bufPool[frameNo] = file->readPage(pageNo);
        hashTable->insert(file, pageNo, frameNo);
        bufDescTable[frameNo].Set(file, pageNo);
        page = &bufPool[frameNo];
    }
}

 /**
 * 
 */
void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
    FrameId frameNo = 0;
    try {
        bool resbitCheck = hashTable->lookup(file, pageNo, frameNo);
        if(resbitCheck == false) {
            return;
        }

        //set dirty bit if dirty is true
        if(dirty) {
            bufDescTable[frameNo].dirty = true;
        }

        //Throws PAGENOTPINNED if the pin count is already 0.
        if(bufDescTable[frameNo].pinCnt > 0) {
            bufDescTable[frameNo].pinCnt--;
        } else {
            throw PageNotPinnedException(file->filename(), pageNo, frameNo);
        }
        
    }
    catch (const HashNotFoundException &e) { //page is not found
        // do nothing
    }
}

/**
 * 
 */
void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
    //allocate a new frame
    FrameId frameNo;
    allocBuf(frameNo);

    //allocate a new page
    bufPool[frameNo] = file->allocatePage();
    page = &bufPool[frameNo];
    pageNo = page->page_number();

    //insert into tables
    hashTable->insert(file, pageNo, frameNo);
    bufDescTable[frameNo].Set(file, pageNo);

    return;
}

/**
 * 
 */
void BufMgr::flushFile(const File* file) 
{
    //iterate over all buffers
    for (unsigned int i = 0; i < numBufs; i++) {
        if (bufDescTable[i].valid == true && bufDescTable[i].file == file) {
            //check if the page number is invalid
            if (bufDescTable[i].pageNo == Page::INVALID_NUMBER) {
                throw BadBufferException(i, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
            }

            //check that the pin count has been emptied
            if (bufDescTable[i].pinCnt > 0) {
                throw PagePinnedException(file->filename(), bufDescTable[i].pageNo, i);
            }

            //if the dirty bit is selected
            if (bufDescTable[i].dirty == true) {
                bufDescTable[i].file->writePage(bufPool[i]);
                bufDescTable[i].dirty = false;
            }

            //remove from tables
            hashTable->remove(bufDescTable[i].file, bufDescTable[i].pageNo);
            bufDescTable[i].Clear();
        } else if (bufDescTable[i].valid == false && bufDescTable[i].file == file) {
            BadBufferException(bufDescTable[i].frameNo, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].)
        }
    }
}

/**
 * 
 */
void BufMgr::disposePage(File* file, const PageId PageNo)
{
    FrameId frameNo = 0;
    try {
        //find and check if refbit exists
        bool resbitCheck = hashTable->lookup(file, PageNo, frameNo);
        if (true == resbitCheck) {
        //if it does, remove file with specified frameNo and pageNo from table
        hashTable->remove(bufDescTable[frameNo].file, bufDescTable[frameNo].pageNo);
        bufDescTable[frameNo].Clear();
        }
        //deallocate the page in the file
        file->deletePage(PageNo);
    } catch (const HashNotFoundException &e) { //page is not found
        // do nothing
    }
    return;
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
