# 564p2
Functions to implement:

Ziwei is working on them:
{
BufMgr::~BufMgr()
void BufMgr::advanceClock()
void BufMgr::allocBuf(FrameId & frame)
}

void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)

void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 

void BufMgr::flushFile(const File* file) 

void BufMgr::disposePage(File* file, const PageId PageNo)
