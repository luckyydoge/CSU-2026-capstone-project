from sqlalchemy import Column, Integer, String, DateTime, func, Boolean, Float, ForeignKey, Text
from sqlalchemy.dialects.sqlite import JSON
from sqlalchemy.orm import relationship
from app.database import Base


class Stage(Base):
    __tablename__ = "stages"

    name = Column(String(255), primary_key=True)
    description = Column(Text)
    handler = Column(String(500), nullable=False)
    input_type = Column(String(100), nullable=False)
    output_type = Column(String(100), nullable=False)
    input_schema = Column(JSON)
    output_schema = Column(JSON)
    model_name = Column(String(255))
    config = Column(JSON)
    dependencies = Column(JSON)
    runtime_env = Column(JSON)
    can_split = Column(Boolean, default=False)
    is_deployable = Column(Boolean, default=True)
    created_at = Column(DateTime, server_default=func.now())

    deployment_config = relationship("DeploymentConfig", uselist=False, back_populates="stage")
    application_stages = relationship("ApplicationStage", back_populates="stage")
    application_edges_from = relationship("ApplicationEdge", foreign_keys="ApplicationEdge.from_stage", back_populates="from_stage_ref")
    application_edges_to = relationship("ApplicationEdge", foreign_keys="ApplicationEdge.to_stage", back_populates="to_stage_ref")
    application_entries = relationship("ApplicationEntry", back_populates="stage")
    application_exits = relationship("ApplicationExit", back_populates="stage")


class Application(Base):
    __tablename__ = "applications"

    app_id = Column(String(36), primary_key=True)
    name = Column(String(255), unique=True, nullable=False)
    description = Column(Text)
    input_type = Column(String(100), nullable=False)
    created_at = Column(DateTime, server_default=func.now())

    application_stages = relationship("ApplicationStage", back_populates="application", cascade="all, delete-orphan")
    application_edges = relationship("ApplicationEdge", back_populates="application", cascade="all, delete-orphan")
    application_entries = relationship("ApplicationEntry", back_populates="application", cascade="all, delete-orphan")
    application_exits = relationship("ApplicationExit", back_populates="application", cascade="all, delete-orphan")
    tasks = relationship("Task", back_populates="application")


class ApplicationStage(Base):
    __tablename__ = "application_stages"

    app_id = Column(String(36), ForeignKey("applications.app_id", ondelete="CASCADE"), primary_key=True)
    stage_name = Column(String(255), ForeignKey("stages.name", ondelete="CASCADE"), primary_key=True)
    order_index = Column(Integer, nullable=False)

    application = relationship("Application", back_populates="application_stages")
    stage = relationship("Stage", back_populates="application_stages")


class ApplicationEdge(Base):
    __tablename__ = "application_edges"

    app_id = Column(String(36), ForeignKey("applications.app_id", ondelete="CASCADE"), primary_key=True)
    from_stage = Column(String(255), ForeignKey("stages.name"), primary_key=True)
    to_stage = Column(String(255), ForeignKey("stages.name"), primary_key=True)
    condition = Column(String(255))
    weight = Column(Float)
    is_split_point = Column(Boolean, default=False)

    application = relationship("Application", back_populates="application_edges")
    from_stage_ref = relationship("Stage", foreign_keys=[from_stage], back_populates="application_edges_from")
    to_stage_ref = relationship("Stage", foreign_keys=[to_stage], back_populates="application_edges_to")


class ApplicationEntry(Base):
    __tablename__ = "application_entries"

    app_id = Column(String(36), ForeignKey("applications.app_id", ondelete="CASCADE"), primary_key=True)
    stage_name = Column(String(255), ForeignKey("stages.name"), primary_key=True)

    application = relationship("Application", back_populates="application_entries")
    stage = relationship("Stage", back_populates="application_entries")


class ApplicationExit(Base):
    __tablename__ = "application_exits"

    app_id = Column(String(36), ForeignKey("applications.app_id", ondelete="CASCADE"), primary_key=True)
    stage_name = Column(String(255), ForeignKey("stages.name"), primary_key=True)

    application = relationship("Application", back_populates="application_exits")
    stage = relationship("Stage", back_populates="application_exits")


class DeploymentConfig(Base):
    __tablename__ = "deployment_configs"

    stage_name = Column(String(255), ForeignKey("stages.name", ondelete="CASCADE"), primary_key=True)
    allowed_tiers = Column(JSON, nullable=False)
    resources = Column(JSON, nullable=False)
    replicas = Column(Integer, default=1)
    node_affinity = Column(JSON)
    proximity = Column(JSON)
    description = Column(Text)
    created_at = Column(DateTime, server_default=func.now())

    stage = relationship("Stage", back_populates="deployment_config")


class Strategy(Base):
    __tablename__ = "strategies"

    name = Column(String(255), primary_key=True)
    strategy_type = Column(String(50), nullable=False)
    handler = Column(String(500), nullable=False)
    config = Column(JSON)
    description = Column(Text)
    created_at = Column(DateTime, server_default=func.now())

    tasks = relationship("Task", back_populates="strategy")


class Task(Base):
    __tablename__ = "tasks"

    task_id = Column(String(36), primary_key=True)
    app_name = Column(String(255), ForeignKey("applications.name", ondelete="CASCADE"), nullable=False)
    strategy_name = Column(String(255), ForeignKey("strategies.name", ondelete="CASCADE"), nullable=False)
    input_data_uri = Column(Text)
    final_output_uri = Column(Text)
    status = Column(String(20), nullable=False)
    created_at = Column(DateTime, server_default=func.now())
    completed_at = Column(DateTime)

    application = relationship("Application", back_populates="tasks")
    strategy = relationship("Strategy", back_populates="tasks")
    execution_traces = relationship("ExecutionTrace", back_populates="task", cascade="all, delete-orphan")


class ExecutionTrace(Base):
    __tablename__ = "execution_traces"

    trace_id = Column(Integer, primary_key=True, autoincrement=True)
    task_id = Column(String(36), ForeignKey("tasks.task_id", ondelete="CASCADE"))
    step_index = Column(Integer, nullable=False)
    stage_name = Column(String(255), nullable=False)
    node_id = Column(String(255))
    node_tier = Column(String(20))
    start_time = Column(DateTime)
    execute_time = Column(DateTime)
    end_time = Column(DateTime)
    queue_time_ms = Column(Float)
    actual_exec_time_ms = Column(Float)
    execution_time_ms = Column(Float)
    transfer_time_ms = Column(Float)
    input_size_bytes = Column(Integer)
    output_size_bytes = Column(Integer)
    cpu_percent = Column(Float)
    memory_mb = Column(Integer)
    error_msg = Column(Text)

    task = relationship("Task", back_populates="execution_traces")


class MonitorRecord(Base):
    __tablename__ = "monitor_records"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    status = Column(String, default="active")
    created_at = Column(DateTime, server_default=func.now())


class File(Base):
    __tablename__ = "files"

    file_id = Column(String(255), primary_key=True)
    filename = Column(String(500), nullable=False)
    file_type = Column(String(100), nullable=False)
    size_bytes = Column(Integer, nullable=False)
    file_path = Column(String(500), nullable=False)
    created_at = Column(DateTime, server_default=func.now())
