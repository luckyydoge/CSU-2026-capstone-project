from typing import List, Optional
import uuid

class Submission:
    def __init__(self) -> None:
        self.id = id if id else str(uuid.uuid4())[:8]

class Job:
    def __init__(self, id: str,
                 tasks: Optional[List[Task]] = None,
                 nodes: Optional[List[Node]] = None) -> None:
        self.id = id
        self.tasks = tasks
        self.nodes = nodes
class Task:
    def __init__(self, id: str, job_id: str = "Unknown", worker: Optional[Worker] = None) -> None:
        self.id = id
        self.job_id = job_id
        self.worker = worker

class Worker:
    def __init__(self, id: str, cpu_usage: float, memory_usage: float) -> None:
        self.id = id
        self.cpu_usage = cpu_usage
        self.memory_usage = memory_usage

class Node:
    def __init__(self, id: str ="Unknown",
                 cpu_usage_percent: float = 0,
                 memory_usage_percent: float = 0,
                 memory_total: int = 0,
                 memory_usage: int = 0) -> None:
        self.id = id
        self.cpu_usage_percent = cpu_usage_percent
        self.memory_total = memory_total
        self.memory_usage = memory_usage
        self.memory_usage_percent = memory_usage_percent
    
