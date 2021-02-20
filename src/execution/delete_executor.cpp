//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/delete_executor.h"
#include "execution/executor_factory.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void DeleteExecutor::Init() {
  table_meta_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  index_info_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_meta_->name_)[0];
  child_executor_ = ExecutorFactory::CreateExecutor(GetExecutorContext(), plan_->GetChildPlan());
  child_executor_->Init();
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple tuple_to_delete;
  RID rid_to_delete;
  while(child_executor_->Next(&tuple_to_delete, &rid_to_delete)){
    // 加了MarkDelete会有问题，可能这个函数是用在事务那的
    // table_meta_->table_->MarkDelete(rid_to_delete, exec_ctx_->GetTransaction());
    table_meta_->table_->ApplyDelete(rid_to_delete, exec_ctx_->GetTransaction());
    Tuple index_to_delete = tuple_to_delete.KeyFromTuple(table_meta_->schema_, index_info_->key_schema_, index_info_->index_->GetKeyAttrs());
    index_info_->index_->DeleteEntry(index_to_delete, rid_to_delete, exec_ctx_->GetTransaction());
  };
  return false;
}

}  // namespace bustub
