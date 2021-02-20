//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const {
  return root_page_id_ == INVALID_PAGE_ID;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  // Tree empty
  if(IsEmpty()) return false;
  // find leaf page -> find value in leaf page
  Page* leaf_page = FindLeafPage(key, false, transaction, true);
  auto leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(leaf_page->GetData());
  ValueType value;
  if(leaf->Lookup(key, &value, comparator_)){
    result->emplace_back(value);
    UnlockPage(leaf_page, transaction, true);
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    return true;
  }
  UnlockPage(leaf_page, transaction, true);
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  if (IsEmpty()){
    StartNewTree(key, value, transaction);
    return true;
  }
  return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value, Transaction *transaction) {
  Page* page = buffer_pool_manager_->NewPage(&root_page_id_);
  LockPage(page, transaction, false);
  if (page == nullptr) {throw Exception(ExceptionType::OUT_OF_MEMORY, "buffer out of memory");}
  auto root = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(page->GetData());
  root->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
  root->Insert(key, value, comparator_);
  UpdateRootPageId(1);
  UnlockPage(page, transaction, false);
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  // TODO：现在有一个问题，从FindLeafPage找到一个叶子节点并插入新值后，需要将其写回磁盘，
  //  这时需要用到Unpin或者Flush，但万一这个page被替换出缓存了怎么办，应该需要再Fetch一下
  //  answer:有LockPage存在，所以page不会被替换出去
  Page* leaf_page = FindLeafPage(key, false, transaction, false);
  auto leaf_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(leaf_page->GetData());
  ValueType temp;
  // 处理重复值
  if (leaf_node->Lookup(key, &temp, comparator_)) {
    UnlockPage(leaf_page, transaction, false);
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return false;
  }
  int leaf_size = leaf_node->Insert(key, value, comparator_);
  // 超出容量, split
  if (leaf_size > leaf_max_size_){
    // 分裂 -> middle key上移到parent -> 更新 leaf next_page_id
    B_PLUS_TREE_LEAF_PAGE_TYPE* new_leaf_node = Split(leaf_node, transaction);
    KeyType middle_key = new_leaf_node->KeyAt(0);
    InsertIntoParent(leaf_node, middle_key, new_leaf_node, transaction);
    // 注意新创建的page的next要指向原page的next，类似链表插入
    new_leaf_node->SetNextPageId(leaf_node->GetNextPageId());
    leaf_node->SetNextPageId(new_leaf_node->GetPageId());
  }
  UnlockPage(leaf_page, transaction, false);
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node, Transaction *transaction) {
  page_id_t new_page_id;
  Page* new_page = buffer_pool_manager_->NewPage(&new_page_id);
  if (new_page == nullptr) {throw Exception(ExceptionType::OUT_OF_MEMORY, "Split: out of memory");}
  LockPage(new_page, transaction, false);
  auto new_node = reinterpret_cast<N*>(new_page->GetData());
  // 从bufferpool中new的page，要进行初始化
  new_node->Init(new_page_id, node->GetParentPageId(), node->GetMaxSize());
  // TODO: internal page和 leaf page的 MoveHalfTo()参数不同，修改了头文件，强行使参数一致
  node->MoveHalfTo(new_node, buffer_pool_manager_);
  UnlockPage(new_page, transaction, false);
  buffer_pool_manager_->UnpinPage(new_page_id, true);
  return new_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  if (old_node->IsRootPage()){
    page_id_t new_root_id;
    Page* new_root_page = buffer_pool_manager_->NewPage(&new_root_id);
    LockPage(new_root_page, transaction, false);
    auto new_root = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>(new_root_page->GetData());
    new_root->Init(new_root_id, INVALID_PAGE_ID, internal_max_size_);
    new_root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    old_node->SetParentPageId(new_root_id);
    new_node->SetParentPageId(new_root_id);
    root_page_id_ = new_root_id;
    UnlockPage(new_root_page, transaction, false);
    buffer_pool_manager_->UnpinPage(new_root_id, true);
    return;
  }
  page_id_t parent_id = old_node->GetParentPageId();
  Page* parent_page = buffer_pool_manager_->FetchPage(parent_id);
  LockPage(parent_page, transaction, false);
  // 这里需要指定 ValueType 为 page_id_t，不然会报错，因为InsertNodeAfter传入的参数是page_id_t(ValueType)，两者要统一
  auto parent_node = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>* >(parent_page->GetData());
  // old value : old_node page_id, new key: middle key, new_value: new_node page_id
  int parent_size = parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
  // 超出容量, split
  if (parent_size > internal_max_size_){
    // 分裂 -> middle key上移到parent
    BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>* new_internal = Split(parent_node, transaction);
    KeyType middle_key = new_internal->KeyAt(0);
    InsertIntoParent(parent_node, middle_key, new_internal);
  }
  UnlockPage(parent_page, transaction, false);
  buffer_pool_manager_->UnpinPage(parent_id, true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) {return;}
  Page* leaf_page = FindLeafPage(key, false, transaction, false);
  auto leaf_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(leaf_page->GetData());
  int size = leaf_node->RemoveAndDeleteRecord(key, comparator_);
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  UnlockPage(leaf_page, transaction, false);

  if (size < leaf_node->GetMinSize()){
    // 更新next_page_id和parent_page的array这些操作交给CoalesceOrRedistribute来做
    bool need_delete = CoalesceOrRedistribute(leaf_node, transaction);
    if (need_delete){
      page_id_t delete_page = leaf_node->GetPageId();
      bool deleted = buffer_pool_manager_->DeletePage(delete_page);
      transaction->AddIntoDeletedPageSet(delete_page);
      printf("buffer pool delete page:%d success(1) or fail(0): %d\n", delete_page, deleted);
    }
  }
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  if (node->IsRootPage()){
    return AdjustRoot(node);
  }
  // 找到parent
  page_id_t parent_id = node->GetParentPageId();
  Page* parent_page = buffer_pool_manager_->FetchPage(parent_id);
  LockPage(parent_page, transaction, false);
  auto parent_node = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>(parent_page->GetData());
  // 向parent找sibling
  page_id_t cur_id = node->GetPageId();
  int cur_index = parent_node->ValueIndex(cur_id);
  // 默认先找左兄弟，没有在找右兄弟
  int is_left_sib = 1;
  int sib_index = cur_index-1;
  if (cur_index == 0) {
    sib_index = 1;
    is_left_sib = 0;
  }
  page_id_t sib_id = parent_node->ValueAt(sib_index);
//  printf("get sib page id: %d\n", static_cast<int>(sib_id));
  Page* sib_page = buffer_pool_manager_->FetchPage(sib_id);
  LockPage(sib_page, transaction, false);
  auto sib_node = reinterpret_cast<N*>(sib_page->GetData());
  // 获取sib_node后判断是Coalesce还是Redistribute
  if (sib_node->GetSize() + node->GetSize() > node->GetMaxSize()){
    Redistribute(sib_node, node, is_left_sib);
    UnlockPage(parent_page, transaction, false);
    UnlockPage(sib_page, transaction, false);
    buffer_pool_manager_->UnpinPage(parent_id, true);
    buffer_pool_manager_->UnpinPage(sib_id, true);
    return false;
  }
  bool parent_need_delete = Coalesce(&sib_node, &node, &parent_node, is_left_sib);
  UnlockPage(parent_page, transaction, false);
  UnlockPage(sib_page, transaction, false);
  buffer_pool_manager_->UnpinPage(parent_id, true);
  buffer_pool_manager_->UnpinPage(sib_id, true);
  // 由于调用方法的是node，而要删除的是sib_node，无法BPlusTree::Remove在中完成，
  // 因此在这里将sib_node删掉，并返回false，表示node不用删
  if (!is_left_sib){
    page_id_t delete_page_id = node->GetPageId();
    bool deleted = buffer_pool_manager_->DeletePage(delete_page_id);
    transaction->AddIntoDeletedPageSet(delete_page_id);
    printf("buffer pool delete page:%d success(1) or fail(0) in CoalesceOrRedistribute function: %d\n", delete_page_id, deleted);
    return false;
  }
  if(parent_need_delete){
    bool deleted = buffer_pool_manager_->DeletePage(parent_id);
    transaction->AddIntoDeletedPageSet(parent_id);
    printf("buffer pool delete parent page: %d success(1) or fail(0) in CoalesceOrRedistribute function: %d\n", parent_id, deleted);
  }
  return true;
}

/*
 * 全给兄弟
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {
  KeyType temp;
  int remove_index;
  // 这个二重指针用的好，目的在于能让我进行swap操作
  // 这样就免除了向左merge和向右merge的操作不一致
  // 不仅可以简化下面的代码，同时可以让BPlusTree::CoalesceOrRedistribute()的Bufferpool能够正确的删除page
  if (!index){
    N *p = *neighbor_node;
    *neighbor_node = *node;
    *node = p;
//    printf("swap, node point to page: %d, neighbor point to page: %d\n", (*node)->GetPageId(), (*neighbor_node)->GetPageId());
  }
  if (!(*node)->IsLeafPage()){
    int index_in_parent = (*parent)->ValueIndex((*node)->GetPageId());
    temp = (*parent)->KeyAt(index_in_parent);
  }
  (*node)->MoveAllTo(*neighbor_node, temp, buffer_pool_manager_);
  remove_index = (*parent)->ValueIndex((*node)->GetPageId());
  (*parent)->Remove(remove_index);
  if ((*parent)->GetSize() < (*parent)->GetMinSize()){
    return CoalesceOrRedistribute(*parent);
  }
  return false;
}

/*
 * 从兄弟那借
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  KeyType key;
  if (!node->IsLeafPage()){
    page_id_t parent_id = node->GetParentPageId();
    Page* page = buffer_pool_manager_->FetchPage(parent_id);
    auto parent_node = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>(page->GetData());
    key = parent_node->KeyAt(parent_node->ValueIndex(node->GetPageId()));
    buffer_pool_manager_->UnpinPage(parent_id, false);
  }
  // 由于这个方法是InternalPage和LeafPage的公用方法，因此更新parent_id和array的操作放到InternalPage自己的方法中去
  if (index){
    neighbor_node->MoveLastToFrontOf(node, key, buffer_pool_manager_);
  }else{
    // index为0，代表是右边的sib
    neighbor_node->MoveFirstToEndOf(node, key, buffer_pool_manager_);
  }
  if (neighbor_node->GetSize() < neighbor_node->GetMinSize()) {
    CoalesceOrRedistribute(neighbor_node);
  }
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  if(!old_root_node->IsLeafPage()){
    // Root is not leaf page and have only one child
    if(old_root_node->GetSize() == 1){
      auto root_node = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(old_root_node);
      root_page_id_ = root_node->RemoveAndReturnOnlyChild();
      UpdateRootPageId(false);
      auto page = buffer_pool_manager_->FetchPage(root_page_id_);
      auto new_root = reinterpret_cast<BPlusTreePage *>(page->GetData());
      new_root->SetParentPageId(INVALID_PAGE_ID);
      buffer_pool_manager_->UnpinPage(root_page_id_, true);
      return true;
    }
    return false;
  }else{
    //Root is leaf page, delete when size == 0
    if(old_root_node->GetSize() == 0){
      root_page_id_ = INVALID_PAGE_ID;
      UpdateRootPageId(false);
      return true;
    }
    return false;
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() {
  KeyType key;
  Page* page = FindLeafPage(key, true, nullptr, true);
  auto node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(page->GetData());
  buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
  UnlockPage(page, nullptr, true);
  return INDEXITERATOR_TYPE(node, 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  Page* page = FindLeafPage(key, false, nullptr, true);
  auto node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(page->GetData());
  int index = node->KeyIndex(key, comparator_);
  buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
  UnlockPage(page, nullptr, true);
  return INDEXITERATOR_TYPE(node, index, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() {
  return INDEXITERATOR_TYPE(nullptr, -1, buffer_pool_manager_);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key 
 * if leftMost flag == true, find the left most leaf page (whether it contains the key)
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost, Transaction* txn, bool read) {
  // 修改思路，Unpin和Unlock操作不再由FindLeafPage自己完成，而是交给调用他的函数完成
  // 若read为true,那么返回的Page是带有R锁，否则是带有W锁
  Page* page = buffer_pool_manager_->FetchPage(root_page_id_);
  LockPage(page, txn, true);
  // since don't know the correct page type, so first reinterpret to parent class(BPlusTreePage)
  auto node = reinterpret_cast<BPlusTreePage*>(page->GetData());
  while(!node->IsLeafPage()){
    auto internal = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>(node);
    // 1. 找到下一个节点id
    page_id_t child_page_id;
    if (leftMost){
      child_page_id = internal->ValueAt(0);
    }else{
      child_page_id = internal->Lookup(key, comparator_);
    }
    // 2. Unpin Unlock当前节点
    UnlockPage(page, txn, true);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    // 3. 得到下个节点并Lock
    page = buffer_pool_manager_->FetchPage(child_page_id);
    LockPage(page, txn, true);
    node = reinterpret_cast<BPlusTreePage*>(page->GetData());
  }
//  UnlockPage(page, txn, true);
//  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  if (!read){
    UnlockPage(page, txn, true);
    LockPage(page, txn, false);
  }
  return page;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::LockPage(Page* page, Transaction* txn, bool read){
  if(read){
    page->RLatch();
  }else{
    page->WLatch();
  }
  if(txn != nullptr)
    txn->GetPageSet()->push_back(page);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnlockPage(Page* page, Transaction* txn, bool read){
  if(read){
    page->RUnlatch();
  }else{
    page->WUnlatch();
  }
  if(txn != nullptr)
    txn->GetPageSet()->pop_front();
}


/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
