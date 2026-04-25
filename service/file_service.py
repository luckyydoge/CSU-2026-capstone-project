import os
import uuid
import shutil
from datetime import datetime
from typing import Dict, List, Optional

class FileService:
    UPLOAD_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "uploads")

    @staticmethod
    def init():
        os.makedirs(FileService.UPLOAD_DIR, exist_ok=True)

    @staticmethod
    def save_file(filename: str, content: bytes) -> Dict:
        file_id = str(uuid.uuid4())
        ext = os.path.splitext(filename)[1] if filename else ""
        stored_filename = f"{file_id}{ext}"
        file_path = os.path.join(FileService.UPLOAD_DIR, stored_filename)

        with open(file_path, "wb") as f:
            f.write(content)

        file_info = {
            "file_id": file_id,
            "original_filename": filename,
            "stored_filename": stored_filename,
            "file_path": file_path,
            "size": len(content),
            "upload_time": datetime.utcnow().isoformat()
        }

        from storage.memory_store import FILE_DB
        FILE_DB[file_id] = file_info

        return file_info

    @staticmethod
    def get_file(file_id: str) -> Optional[Dict]:
        from storage.memory_store import FILE_DB
        return FILE_DB.get(file_id)

    @staticmethod
    def get_file_path(file_id: str) -> Optional[str]:
        file_info = FileService.get_file(file_id)
        if file_info and os.path.exists(file_info["file_path"]):
            return file_info["file_path"]
        return None

    @staticmethod
    def delete_file(file_id: str) -> bool:
        file_info = FileService.get_file(file_id)
        if not file_info:
            return False

        try:
            if os.path.exists(file_info["file_path"]):
                os.remove(file_info["file_path"])
            from storage.memory_store import FILE_DB
            FILE_DB.pop(file_id, None)
            return True
        except Exception:
            return False

    @staticmethod
    def list_files() -> List[Dict]:
        from storage.memory_store import FILE_DB
        return list(FILE_DB.values())

    @staticmethod
    def get_file_content(file_id: str) -> Optional[bytes]:
        file_path = FileService.get_file_path(file_id)
        if file_path:
            with open(file_path, "rb") as f:
                return f.read()
        return None

FileService.init()
