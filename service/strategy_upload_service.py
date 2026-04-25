# service/strategy_upload_service.py
import os
import hashlib
import importlib
from typing import Dict, List, Optional, Tuple

STRATEGY_CODE_DIR = "strategy_code"
MAX_FILE_SIZE = 10 * 1024 * 1024  # 10MB
ALLOWED_EXTENSIONS = {'.py'}


class StrategyUploadService:
    
    @staticmethod
    def ensure_strategy_code_dir():
        """确保strategy_code目录存在"""
        os.makedirs(STRATEGY_CODE_DIR, exist_ok=True)
    
    @staticmethod
    def sanitize_filename(filename: str) -> str:
        """安全处理文件名"""
        safe_filename = os.path.basename(filename)
        if not safe_filename:
            return "uploaded_strategy.py"
        
        # 移除危险字符
        safe_filename = ''.join(c for c in safe_filename if c.isalnum() or c in '._-')
        if not safe_filename.endswith('.py'):
            safe_filename += '.py'
        
        return safe_filename
    
    @staticmethod
    def validate_python_code(content: bytes) -> Tuple[bool, str]:
        """验证Python代码语法"""
        try:
            code_str = content.decode('utf-8')
            compile(code_str, '<string>', 'exec')
            return True, "Valid Python code"
        except SyntaxError as e:
            return False, f"Syntax error: {e}"
        except UnicodeDecodeError:
            return False, "File encoding must be UTF-8"
    
    @staticmethod
    def calculate_file_hash(content: bytes) -> str:
        """计算文件哈希值"""
        return hashlib.sha256(content).hexdigest()
    
    @staticmethod
    def check_file_conflicts(filename: str, file_hash: str) -> Tuple[bool, str]:
        """检查文件冲突"""
        file_path = os.path.join(STRATEGY_CODE_DIR, filename)
        
        if not os.path.exists(file_path):
            return False, "No conflict"
        
        # 检查内容是否相同
        with open(file_path, 'rb') as f:
            existing_hash = hashlib.sha256(f.read()).hexdigest()
        
        if existing_hash == file_hash:
            return True, "File with same content already exists"
        else:
            return True, "File with same name but different content already exists"
    
    @staticmethod
    def generate_unique_filename(base_filename: str, file_hash: str) -> str:
        """生成唯一文件名"""
        name, ext = os.path.splitext(base_filename)
        return f"{name}_{file_hash[:8]}{ext}"
    
    @staticmethod
    def validate_file(filename: str, content: bytes) -> Tuple[bool, str]:
        """验证文件"""
        # 检查文件名
        if not filename:
            return False, "No filename provided"
        
        # 检查文件扩展名
        file_ext = os.path.splitext(filename)[1].lower()
        if file_ext not in ALLOWED_EXTENSIONS:
            return False, f"Only {', '.join(ALLOWED_EXTENSIONS)} files are allowed"
        
        # 检查文件大小
        if len(content) > MAX_FILE_SIZE:
            return False, f"File size exceeds maximum allowed size of {MAX_FILE_SIZE // (1024*1024)}MB"
        
        if len(content) == 0:
            return False, "File is empty"
        
        # 验证Python代码语法
        is_valid, validation_msg = StrategyUploadService.validate_python_code(content)
        if not is_valid:
            return False, f"Invalid Python code: {validation_msg}"
        
        return True, "File is valid"
    
    @staticmethod
    def upload_file(filename: str, content: bytes) -> Dict:
        """
        上传策略文件到strategy_code目录
        
        Args:
            filename: 原始文件名
            content: 文件内容
            
        Returns:
            包含上传结果的字典
        """
        StrategyUploadService.ensure_strategy_code_dir()
        
        # 验证文件
        is_valid, validation_msg = StrategyUploadService.validate_file(filename, content)
        if not is_valid:
            raise ValueError(validation_msg)
        
        # 计算文件哈希
        file_hash = StrategyUploadService.calculate_file_hash(content)
        
        # 安全处理文件名
        safe_filename = StrategyUploadService.sanitize_filename(filename)
        
        # 检查文件冲突
        has_conflict, conflict_msg = StrategyUploadService.check_file_conflicts(safe_filename, file_hash)
        
        if has_conflict:
            if "same content" in conflict_msg:
                # 内容相同，返回已存在的文件信息
                module_name = safe_filename[:-3]
                return {
                    "filename": safe_filename,
                    "module_name": module_name,
                    "message": "File with identical content already exists",
                    "duplicate": True,
                    "conflict_type": "content_match"
                }
            else:
                # 文件名冲突但内容不同，生成唯一文件名
                safe_filename = StrategyUploadService.generate_unique_filename(safe_filename, file_hash)
        
        # 保存文件
        file_path = os.path.join(STRATEGY_CODE_DIR, safe_filename)
        try:
            with open(file_path, "wb") as buffer:
                buffer.write(content)
        except Exception as e:
            raise ValueError(f"Failed to save file: {str(e)}")
        
        # 尝试导入验证模块
        module_name = safe_filename[:-3]
        try:
            importlib.invalidate_caches()
            module = importlib.import_module(module_name)
        except Exception as e:
            # 即使导入失败也保存文件，因为可能缺少依赖
            pass
        
        return {
            "filename": safe_filename,
            "module_name": module_name,
            "message": "Upload successful. Use handler format: f'{module_name}:function_name'",
            "duplicate": False,
            "conflict_type": "none",
            "file_hash": file_hash,
            "file_size": len(content)
        }
    
    @staticmethod
    def list_uploaded_files() -> Dict:
        """
        列出所有已上传的策略文件
        
        Returns:
            包含文件列表和总数的字典
        """
        StrategyUploadService.ensure_strategy_code_dir()
        
        files = []
        for filename in os.listdir(STRATEGY_CODE_DIR):
            if filename.endswith('.py'):
                file_path = os.path.join(STRATEGY_CODE_DIR, filename)
                file_stat = os.stat(file_path)
                
                with open(file_path, 'rb') as f:
                    file_hash = hashlib.sha256(f.read()).hexdigest()
                
                files.append({
                    "filename": filename,
                    "module_name": filename[:-3],
                    "file_size": file_stat.st_size,
                    "upload_time": file_stat.st_mtime,
                    "file_hash": file_hash
                })
        
        return {
            "files": files,
            "total": len(files)
        }
    
    @staticmethod
    def delete_uploaded_file(filename: str) -> Dict:
        """
        删除已上传的策略文件
        
        Args:
            filename: 要删除的文件名
            
        Returns:
            包含删除结果的字典
        """
        StrategyUploadService.ensure_strategy_code_dir()
        
        # 安全处理文件名
        safe_filename = StrategyUploadService.sanitize_filename(filename)
        file_path = os.path.join(STRATEGY_CODE_DIR, safe_filename)
        
        if not os.path.exists(file_path):
            raise ValueError("File not found")
        
        try:
            os.remove(file_path)
            return {
                "filename": safe_filename,
                "message": "File deleted successfully"
            }
        except Exception as e:
            raise ValueError(f"Failed to delete file: {str(e)}")
    
    @staticmethod
    def get_file_info(filename: str) -> Optional[Dict]:
        """
        获取策略文件信息
        
        Args:
            filename: 文件名
            
        Returns:
            文件信息字典，如果文件不存在则返回None
        """
        StrategyUploadService.ensure_strategy_code_dir()
        
        safe_filename = StrategyUploadService.sanitize_filename(filename)
        file_path = os.path.join(STRATEGY_CODE_DIR, safe_filename)
        
        if not os.path.exists(file_path):
            return None
        
        file_stat = os.stat(file_path)
        
        with open(file_path, 'rb') as f:
            file_hash = hashlib.sha256(f.read()).hexdigest()
        
        return {
            "filename": safe_filename,
            "module_name": safe_filename[:-3],
            "file_size": file_stat.st_size,
            "upload_time": file_stat.st_mtime,
            "file_hash": file_hash
        }
