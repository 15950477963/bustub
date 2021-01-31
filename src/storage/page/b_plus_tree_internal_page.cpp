//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  return array[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  array[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  for(int i = 0; i < GetSize(); i++) {
    if(array[i].second == value) {
      return i;
    }
  }
  return -1;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
  return array[index].second;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  ValueType page_id = ValueAt(0);
  for(int i = 1; i < GetSize(); i++) {
    if (comparator(key, KeyAt(i)) < 0) {
      break;
    }
    page_id = ValueAt(i);
  }
  return page_id;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  array[0].second = old_value;
  array[1] = std::make_pair(new_key, new_value);
  IncreaseSize(2);
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
  int insert_index = ValueIndex(old_value);
  // TODO:由于不是拷贝不变类型(TriviallyCopyable)，使用mem系列函数会报警，这样做的危害是什么?
  // memmove(array+insert_index+2, array+insert_index+1, (GetSize()-insert_index-1) * sizeof(MappingType));
  for(int i = GetSize(); i > insert_index+1; i--){
    array[i] = array[i-1];
  }
  array[insert_index+1] = std::make_pair(new_key, new_value);
  IncreaseSize(1);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  int remain_size = GetSize() / 2;
  int remove_size = GetSize() - remain_size;
  recipient->CopyNFrom(array + remain_size, remove_size, buffer_pool_manager);
  IncreaseSize(-1 * remove_size);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  for(int i = 0; i < size; i++){
    array[i] = items[i];
    page_id_t page_id = array[i].second;
    Page* page = buffer_pool_manager->FetchPage(page_id);
    auto node = reinterpret_cast<BPlusTreePage*>(page->GetData());
    node->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(page_id, true);
  }
  IncreaseSize(size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  for (int i = index; i < GetSize(); i++){
    array[i] = array[i+1];
  }
  IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  ValueType child_page_id = ValueAt(0);
  IncreaseSize(-1);
  return child_page_id;
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {

//  printf("move all from page：%d to page: %d\n", GetPageId(), recipient->GetPageId());
  int recipient_size = recipient->GetSize();
  for(int i = 0; i < GetSize(); i++){
    recipient->array[recipient_size+i] = array[i];
  }
  recipient->array[recipient_size].first = middle_key;
  recipient->IncreaseSize(GetSize());

  for (int i = 0; i < GetSize(); ++i) {
    page_id_t page_id = ValueAt(i);
    Page* page = buffer_pool_manager->FetchPage(page_id);
    auto node = reinterpret_cast<BPlusTreePage*>(page->GetData());
    node->SetParentPageId(recipient->GetPageId());
    buffer_pool_manager->UnpinPage(page_id, true);
  }
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {
  MappingType pair = array[0];
  pair.first = middle_key;
  recipient->CopyLastFrom(pair, buffer_pool_manager);
  for (int i = 0; i < GetSize()-1; i++){
    array[i] = array[i+1];
  }
  IncreaseSize(-1);
  // 更新parent中指向此page的key
  page_id_t parent_id = GetParentPageId();
  Page* parent_page = buffer_pool_manager->FetchPage(parent_id);
  auto parent_node = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>(parent_page->GetData());
  int index_point_this_page = parent_node->ValueIndex(GetPageId());
  parent_node->SetKeyAt(index_point_this_page, array[0].first);
  buffer_pool_manager->UnpinPage(parent_id, true);

}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  array[GetSize()] = pair;
  Page* page = buffer_pool_manager->FetchPage(pair.second);
  auto node = reinterpret_cast<BPlusTreePage*>(page->GetData());
  node->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(pair.second, true);
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {
  MappingType pair = array[GetSize()-1];
  recipient->SetKeyAt(0, middle_key);
  recipient->CopyFirstFrom(pair, buffer_pool_manager);
  IncreaseSize(-1);
  // 更新parent中指向recipient page的key, 不是此page
  page_id_t parent_id = recipient->GetParentPageId();
  Page* parent_page = buffer_pool_manager->FetchPage(parent_id);
  auto parent_node = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>(parent_page->GetData());
  int index_point_recipient_page = parent_node->ValueIndex(recipient->GetPageId());
  parent_node->SetKeyAt(index_point_recipient_page, recipient->array[0].first);
  buffer_pool_manager->UnpinPage(parent_id, true);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  for(int i = GetSize(); i > 0; i--){
    array[i] = array[i-1];
  }
  array[0] = pair;
  IncreaseSize(1);

  Page* page = buffer_pool_manager->FetchPage(pair.second);
  auto node = reinterpret_cast<BPlusTreePage*>(page->GetData());
  node->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(pair.second, true);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
