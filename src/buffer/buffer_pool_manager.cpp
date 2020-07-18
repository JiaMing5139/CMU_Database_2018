#include "buffer/buffer_pool_manager.h"

namespace cmudb {

/*
 * BufferPoolManager Constructor
 * When log_manager is nullptr, logging is disabled (for test purpose)
 * WARNING: Do Not Edit This Function
 */
    BufferPoolManager::BufferPoolManager(size_t pool_size,
                                         DiskManager *disk_manager,
                                         LogManager *log_manager)
            : pool_size_(pool_size), disk_manager_(disk_manager),
              log_manager_(log_manager) {
        // a consecutive memory space for buffer pool
        pages_ = new Page[pool_size_];
        page_table_ = new ExtendibleHash<page_id_t, Page *>(BUCKET_SIZE);
        replacer_ = new LRUReplacer<Page *>;
        free_list_ = new std::list<Page *>;

        // put all the pages into free list
        for (size_t i = 0; i < pool_size_; ++i) {
            free_list_->push_back(&pages_[i]);
        }
    }

/*
 * BufferPoolManager Deconstructor
 * WARNING: Do Not Edit This Function
 */
    BufferPoolManager::~BufferPoolManager() {
        delete[] pages_;
        delete page_table_;
        delete replacer_;
        delete free_list_;
    }

/**
 * 1. search hash table.
 *  1.1 if exist, pin the page and return immediately
 *  1.2 if no exist, find a replacement entry from either free list or lru
 *      replacer. (NOTE: always find from free list first)
 * 2. If the entry chosen for replacement is dirty, write it back to disk.
 * 3. Delete the entry for the old page from the hash table and insert an
 * entry for the new page.
 * 4. Update page metadata, read page content from disk file and return page
 * pointer
 */
    Page *BufferPoolManager::FetchPage(page_id_t page_id) {
        Page * page;

        if (page_table_->Find(page_id,page)) {
            // if find the page in buffer . return it directly
            replacer_->Erase(page);
            return page;
        } else {
            // can't find the page in buffer. try to find a free frame to store the data from disk


            if (!free_list_->empty()) {
                // there is free frame in free_list
                page = free_list_->front();
                free_list_->pop_front();
            } else {
                // can't find free frame in free_list . try to replace frame in replacer
                if (!replacer_->Victim(page)) {
                    //all fra me have been pined
                    page = nullptr;
                } else {
                    // find a replaced frame,start to swap it out
                    // if the original data in the page is dirty ,then write it back to disk

                    if (page->is_dirty_) {
                        disk_manager_->WritePage(page->page_id_, page->data_);
                    }
                    // delete it in table
                    page_table_->Remove(page->GetPageId());
                }
            }
            if (page == nullptr) return nullptr;

            page_table_->Insert(page_id,page);
            replacer_->Erase(page);
            page->pin_count_ = 1;
            memset(page->data_, 0, sizeof(page->data_));
            disk_manager_->ReadPage(page_id, page->GetData());
            page->page_id_ = page_id;
            page->is_dirty_ = false;
            return page;
        }

    }

/*
 * Implementation of unpin page
 * if pin_count>0, decrement it and if it becomes zero, put it back to
 * replacer if pin_count<=0 before this call, return false. is_dirty: set the
 * dirty flag of this page
 */
    bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
        Page * page;
        if(!page_table_->Find(page_id,page)) return false;

        if (page->pin_count_ <= 0) return false;
        page->pin_count_ --;
        if(page->pin_count_ == 0)
            replacer_->Insert(page);
        page->is_dirty_ = is_dirty;
        return true;
    }

/*
 * Used to flush a particular page of the buffer pool to disk. Should call the
 * write_page method of the disk manager
 * if page is not found in page table, return false
 * NOTE: make sure page_id != INVALID_PAGE_ID
 */
    bool BufferPoolManager::FlushPage(page_id_t page_id) {
        Page * page;
        if(!page_table_->Find(page_id,page)) return false;
        disk_manager_->WritePage(page->page_id_, page->GetData());
        //   replacer_->Unpin(frameId);
        return false;
    }

/**
 * User should call this method for deleting a page. This routine will call
 * disk manager to deallocate the page. First, if page is found within page
 * table, buffer pool manager should be reponsible for removing this entry out
 * of page table, reseting page metadata and adding back to free list. Second,
 * call disk manager's DeallocatePage() method to delete from disk file. If
 * the page is found within page table, but pin_count != 0, return false
 */
    bool BufferPoolManager::DeletePage(page_id_t page_id) { return false; }

/**
 * User should call this method if needs to create a new page. This routine
 * will call disk manager to allocate a page.
 * Buffer pool manager should be responsible to choose a victim page either
 * from free list or lru replacer(NOTE: always choose from free list first),
 * update new page's metadata, zero out memory and add corresponding entry
 * into page table. return nullptr if all the pages in pool are pinned
 */
    Page *BufferPoolManager::NewPage(page_id_t &page_id) {
        auto pg_id = disk_manager_->AllocatePage();
        //  LOG_INFO("allocate a new page");
        Page * free_page = nullptr;
        if (!free_list_->empty()) {
            free_page = free_list_->front();
            free_list_->pop_front();
        } else {
            //get frame from replacer
            if (!replacer_->Victim(free_page)) {
                //all frame have been pined
                free_page = nullptr;
            } else {
                //write the page in frame back to disk
                //if is dirty
                //if is not dirty
                if (free_page->is_dirty_) {
                    disk_manager_->WritePage(free_page->page_id_, free_page->data_);
                }
                page_table_->Remove(free_page->GetPageId());
            }
        }
        // didn't get any free frame
        if (free_page == nullptr) return nullptr;


        page_id = pg_id;

        memset(free_page->data_, 0, sizeof(free_page->data_));
        free_page->page_id_ = pg_id;
        free_page->is_dirty_ = false;
        free_page->pin_count_ = 1;
        page_table_->Insert(page_id,free_page);
        replacer_->Erase(free_page);
        return free_page;

    }


} // namespace cmudb
