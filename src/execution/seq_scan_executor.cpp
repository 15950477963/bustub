//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan){}

void SeqScanExecutor::Init() {
  auto catalog = exec_ctx_->GetCatalog();
  table_meta_ = catalog->GetTable(plan_->GetTableOid());
  table_iter_ = std::make_unique<TableIterator>(table_meta_->table_->Begin(exec_ctx_->GetTransaction()));
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  while (*table_iter_ != table_meta_->table_->End()) {
    // unique_ptr -> table_iterator -> tuple
    //TableIterator重构了*解引用符号，使其返回的是tuple
    *tuple = **table_iter_;
    *rid = tuple->GetRid();
    ++(*table_iter_);
    // 找到了一个满足predicate条件的或者是没有predicate的，返回true
    if (plan_->GetPredicate() == nullptr || plan_->GetPredicate()->Evaluate(tuple, &table_meta_->schema_).GetAs<bool>()){
      return true;
    }
  }
  return false;
}

}  // namespace bustub
