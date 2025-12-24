import sqlite3
import json
from contextlib import contextmanager
from datetime import datetime
from typing import List, Dict, Any, Optional
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DB_PATH = "positioning.db"

@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        raise
    finally:
        conn.close()
def init_db():
    """Инициализация базы данных - с дебагом"""
    print("=" * 50)
    print("🟢 INIT_DB STARTED")
    print("=" * 50)
    
    try:
        with get_db() as conn:
            print("✅ Database connection established")
            
            # 1. calculated_positions
            print("\n1. Creating calculated_positions...")
            try:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS calculated_positions (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        batch_id TEXT NOT NULL,
                        tag_id TEXT NOT NULL,
                        x REAL NOT NULL,
                        y REAL NOT NULL,
                        z REAL NOT NULL DEFAULT 0.0,
                        accuracy REAL NOT NULL DEFAULT 1.0,
                        calculation_timestamp TEXT NOT NULL
                    )
                """)
                print("   ✅ CREATE TABLE executed")
            except Exception as e:
                print(f"   ❌ Error: {e}")
                import traceback
                traceback.print_exc()
            
            # 2. anchors
            print("\n2. Creating anchors...")
            try:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS anchors (
                        anchor_id TEXT PRIMARY KEY,
                        x REAL NOT NULL,
                        y REAL NOT NULL,
                        z REAL NOT NULL,
                        description TEXT,
                        is_active INTEGER DEFAULT 1,
                        last_calibration TEXT
                    )
                """)
                print("   ✅ CREATE TABLE executed")
            except Exception as e:
                print(f"   ❌ Error: {e}")
            
            # 3. raw_measurements
            print("\n3. Creating raw_measurements...")
            try:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS raw_measurements (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        batch_id TEXT NOT NULL,
                        gateway_id TEXT NOT NULL,
                        measurement_timestamp TEXT NOT NULL,
                        anchor_id TEXT NOT NULL,
                        tag_id TEXT NOT NULL,
                        distance_m REAL NOT NULL
                    )
                """)
                print("   ✅ CREATE TABLE executed")
            except Exception as e:
                print(f"   ❌ Error: {e}")

            # 4. processed_batches
            print("\n3. Creating processed_batches...")
            try:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS processed_batches (
                        batch_id TEXT PRIMARY KEY,
                        gateway_id TEXT NOT NULL,
                        measurement_count INTEGER NOT NULL,
                        processed_at TEXT,
                        status TEXT DEFAULT 'pending'
                    )
                """)
                print("   ✅ CREATE TABLE executed")
            except Exception as e:
                print(f"   ❌ Error: {e}")
            
            # Проверь какие таблицы создались
            print("\n4. Checking created tables...")
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()
            print(f"   📋 Tables in DB: {[row[0] for row in tables]}")  # ← row[0] извлекает имя
            
            # Демо-данные анкеров
            print("\n5. Adding demo anchors...")
            cursor = conn.execute("SELECT COUNT(*) FROM anchors")
            count = cursor.fetchone()[0]
            print(f"   Current anchors count: {count}")
            
            if count == 0:
                print("   Adding demo data...")
                conn.execute("""
                    INSERT INTO anchors (anchor_id, x, y, z, description)
                    VALUES 
                        ('anchor-1', 0.0, 0.0, 0.0, 'Северная стена'),
                        ('anchor-2', 0.0, 1.0, 0.0, 'Южная стена'),
                        ('anchor-3', 0.0, 0.0, 1.0, 'Центральная колонна');
                """)
                print("   ✅ Demo data added")
            
            conn.commit()
            print("\n✅ COMMIT successful")
            
    except Exception as e:
        print(f"\n❌ CRITICAL ERROR in init_db: {e}")
        import traceback
        traceback.print_exc()
        return
    
    print("\n" + "=" * 50)
    print("🟢 INIT_DB COMPLETED")
    print("=" * 50)
    
    # Финальная проверка
    print("\n🔍 Final verification:")
    try:
        import sqlite3
        check_conn = sqlite3.connect(DB_PATH)
        cursor = check_conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        print(f"   Tables on disk: {cursor.fetchall()}")
        check_conn.close()
    except Exception as e:
        print(f"   Verification failed: {e}")

def save_measurements_batch(batch_id: str, gateway_id: str, 
                           measurements: List[Dict[str, Any]]) -> int:
    """Сохранение пакета измерений в БД"""
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Сохраняем метаинформацию о батче
        cursor.execute(
            """INSERT INTO processed_batches 
               (batch_id, gateway_id, measurement_count) 
               VALUES (?, ?, ?)""",
            (batch_id, gateway_id, len(measurements))
        )
        
        # Сохраняем каждое измерение
        for meas in measurements:
            cursor.execute(
                """INSERT INTO raw_measurements 
                   (batch_id, gateway_id, measurement_timestamp, 
                    anchor_id, tag_id, distance_m) 
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (batch_id, gateway_id, meas.get('timestamp'),
                 meas['anchor_id'], meas['tag_id'], meas['distance_m'])
            )
        
        conn.commit()
        return cursor.lastrowid


def get_measurements_for_trilateration(tag_id: str, 
                                      timestamp: datetime) -> List[Dict]:
    """Получение измерений для трилатерации"""
    with get_db() as conn:
        cursor = conn.execute("""
            SELECT anchor_id, distance_m 
            FROM raw_measurements 
            WHERE tag_id = ? AND measurement_timestamp = ?
            ORDER BY anchor_id
        """, (tag_id, timestamp))
        
        return [dict(row) for row in cursor.fetchall()]

def get_latest_position_db(tag_id: str) -> Optional[dict]:
    """Получение последней позиции из БД"""
    with get_db() as conn:
        cursor = conn.execute("""
            SELECT tag_id, x, y, z, calculation_timestamp, accuracy
            FROM calculated_positions 
            WHERE tag_id = ?
            ORDER BY calculation_timestamp DESC 
            LIMIT 1
        """, (tag_id,))
        
        row = cursor.fetchone()
        if row:
            return dict(row)
        return None


def get_position_history_db(
    tag_id: str, 
    start_time: datetime, 
    end_time: datetime, 
    limit: int = 1000
) -> List[dict]:
    """Получение истории позиций"""
    if start_time >= end_time:
        raise ValueError("start_time must be earlier than end_time")
    
    if limit > 10000:
        raise ValueError("Limit cannot exceed 10000")
    
    with get_db() as conn:
        cursor = conn.execute("""
            SELECT tag_id, x, y, z, calculation_timestamp, accuracy
            FROM calculated_positions 
            WHERE tag_id = ? 
                AND calculation_timestamp BETWEEN ? AND ?
            ORDER BY calculation_timestamp ASC
            LIMIT ?
        """, (tag_id, start_time, end_time, limit))
        
        return [dict(row) for row in cursor.fetchall()]

def get_all_anchors() -> List[dict]:
    """Получение всех анкеров"""
    with get_db() as conn:
        cursor = conn.execute("""
            SELECT anchor_id, x, y, z, description, is_active, last_calibration
            FROM anchors
            ORDER BY anchor_id
        """)
        return [dict(row) for row in cursor.fetchall()]


def get_anchor_by_id(anchor_id: str) -> Optional[dict]:
    """Получение анкера по ID"""
    with get_db() as conn:
        cursor = conn.execute("""
            SELECT anchor_id, x, y, z, description, is_active, last_calibration
            FROM anchors
            WHERE anchor_id = ?
        """, (anchor_id,))
        row = cursor.fetchone()
        return dict(row) if row else None

def get_all_anchors() -> List[dict]:
    """Получение всех анкеров"""
    with get_db() as conn:
        cursor = conn.execute("""
            SELECT anchor_id, x, y, z, description, is_active, last_calibration
            FROM anchors
            ORDER BY anchor_id
        """)
        return [dict(row) for row in cursor.fetchall()]

def delete_anchor(anchor_id: str) -> bool:
    """Удаление анкера"""
    with get_db() as conn:
        cursor = conn.execute("DELETE FROM anchors WHERE anchor_id = ?", (anchor_id,))
        conn.commit()
        return cursor.rowcount > 0


def create_or_update_anchor(anchor_data: dict) -> None:
    """Создание или обновление анкера"""
    with get_db() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO anchors 
            (anchor_id, x, y, z, description, is_active, last_calibration)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            anchor_data['anchor_id'],
            anchor_data['x'],
            anchor_data['y'],
            anchor_data['z'],
            anchor_data.get('description'),
            anchor_data.get('is_active', True),
            anchor_data.get('last_calibration')
        ))
        conn.commit()
