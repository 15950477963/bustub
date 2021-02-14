//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/insert_executor.h"
#include "execution/executor_factory.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void InsertExecutor::Init() {
  table_meta_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
//  if (!exec_ctx_->GetCatalog()->GetTableIndexes(table_meta_->name_).empty()){
//    index_info_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_meta_->name_)[0];
//  }
  index_info_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_meta_->name_)[0];
  if (!plan_->IsRawInsert()) {
    child_executor_ = ExecutorFactory::CreateExecutor(GetExecutorContext(), plan_->GetChildPlan());
    child_executor_->Init();
  }
}

// 1. 目前实现的是通过next一次性插入所有记录，感觉有点不太对
// 2. 外部调用insert时没有传入result set，这里也就不需要将*tuple和*rid作为传出值了
bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  // RawInsert代表insert的记录是直接给定的，而不是通过别的查询得到的
  if (plan_->IsRawInsert()) {
    int size = plan_->RawValues().size();
    for (int i=0; i < size; i++) {
      auto values = plan_->RawValuesAt(i);
      Tuple tuple_to_insert {values, &table_meta_->schema_};
      RID rid_to_insert;
      // InsertTuple在tuple过大，page剩余空间放不下时会返回false
      if (table_meta_->table_->InsertTuple(tuple_to_insert, &rid_to_insert, exec_ctx_->GetTransaction())){
        index_info_->index_->InsertEntry(tuple_to_insert, rid_to_insert, exec_ctx_->GetTransaction());
      }else {
        return false;
      }
    }
    return false;
  } else {
    Tuple tuple_to_insert;
    RID rid_to_insert;
    // 注意Next的RID和InsertTuple的RID,两者不一样，
    // 前者是Next找到的符合条件的Tuple的RID，后者是Insert方法将新的Tuple插入的位置
    while (child_executor_->Next(&tuple_to_insert, rid)) {
      if (table_meta_->table_->InsertTuple(tuple_to_insert, &rid_to_insert, exec_ctx_->GetTransaction())) {
        index_info_->index_->InsertEntry(tuple_to_insert.KeyFromTuple(table_meta_->schema_, index_info_->key_schema_, index_info_->index_->GetKeyAttrs()),
                                         rid_to_insert, exec_ctx_->GetTransaction());
      } else {
        return false;
      }
    }

    return false;
  }
  return true;
}

}  // namespace bustub
