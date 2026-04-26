import os
import uuid
from typing import Dict, List, Optional
from app.database import SessionLocal
from app.models import File as FileModel


class FileService:
    UPLOAD_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "uploads")

    @staticmethod
    def init():
        os.makedirs(FileService.UPLOAD_DIR, exist_ok=True)

    @staticmethod
    def save_file(filename: str, content: bytes) -> Dict:
        FileService.init()
        file_id = str(uuid.uuid4())
        ext = os.path.splitext(filename)[1] if filename else ""
        stored_filename = f"{file_id}{ext}"
        file_path = os.path.join(FileService.UPLOAD_DIR, stored_filename)

        with open(file_path, "wb") as f:
            f.write(content)

        # 保存到数据库
        db = SessionLocal()
        try:
            db_file = FileModel(
                file_id=file_id,
                filename=filename,
                file_type="application/octet-stream",
                size_bytes=len(content),
                file_path=file_path
            )
            db.add(db_file)
            db.commit()
            db.refresh(db_file)

            return {
                "file_id": db_file.file_id,
                "original_filename": db_file.filename,
                "stored_filename": stored_filename,
                "file_path": db_file.file_path,
                "size": db_file.size_bytes,
                "upload_time": db_file.created_at.isoformat() if db_file.created_at else None
            }
        finally:
            db.close()

    @staticmethod
    def get_file(file_id: str) -> Optional[Dict]:
        db = SessionLocal()
        try:
            db_file = db.query(FileModel).filter(FileModel.file_id == file_id).first()
            if db_file:
                ext = os.path.splitext(db_file.filename)[1] if db_file.filename else ""
                stored_filename = f"{db_file.file_id}{ext}"
                return {
                    "file_id": db_file.file_id,
                    "original_filename": db_file.filename,
                    "stored_filename": stored_filename,
                    "file_path": db_file.file_path,
                    "size": db_file.size_bytes,
                    "upload_time": db_file.created_at.isoformat() if db_file.created_at else None
                }
            return None
        finally:
            db.close()

    @staticmethod
    def get_file_path(file_id: str) -> Optional[str]:
        file_info = FileService.get_file(file_id)
        if file_info and os.path.exists(file_info["file_path"]):
            return file_info["file_path"]
        return None

    @staticmethod
    def delete_file(file_id: str) -> bool:
        db = SessionLocal()
        try:
            db_file = db.query(FileModel).filter(FileModel.file_id == file_id).first()
            if not db_file:
                return False

            try:
                if os.path.exists(db_file.file_path):
                    os.remove(db_file.file_path)
                db.delete(db_file)
                db.commit()
                return True
            except Exception:
                return False
        finally:
            db.close()

    @staticmethod
    def list_files() -> List[Dict]:
        db = SessionLocal()
        try:
            db_files = db.query(FileModel).all()
            result = []
            for db_file in db_files:
                ext = os.path.splitext(db_file.filename)[1] if db_file.filename else ""
                stored_filename = f"{db_file.file_id}{ext}"
                result.append({
                    "file_id": db_file.file_id,
                    "original_filename": db_file.filename,
                    "stored_filename": stored_filename,
                    "file_path": db_file.file_path,
                    "size": db_file.size_bytes,
                    "upload_time": db_file.created_at.isoformat() if db_file.created_at else None
                })
            return result
        finally:
            db.close()

    @staticmethod
    def get_file_content(file_id: str) -> Optional[bytes]:
        file_path = FileService.get_file_path(file_id)
        if file_path:
            with open(file_path, "rb") as f:
                return f.read()
        return None


FileService.init()
