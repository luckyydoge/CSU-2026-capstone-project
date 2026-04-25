# storage/memory_store.py
STAGE_DB = {}
APPLICATION_DB = {}
DEPLOYMENT_DB = {}   # 新增，key: stage_name, value: dict
# 添加策略存储
STRATEGY_DB = {}   # key: strategy_name, value: dict
TASK_DB = {}      # task_id -> 任务元数据（不含trace）
TRACE_DB = {}     # task_id -> ExecutionTrace dict
FILE_DB = {}       # file_id -> 文件信息
