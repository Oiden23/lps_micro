import sqlite3
import json
from contextlib import contextmanager
from datetime import datetime
from typing import List, Dict, Any, Optional
import logging
from uuid import uuid4, UUID


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DB_PATH = "access_control.db"

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
    print("🟢 ACCESS CONTROL SERVICE INIT_DB STARTED")
    print("=" * 50)
    
    try:
        with get_db() as conn:
            print("✅ Database connection established")
            
            # 1. entities (сущности: сотрудники и оборудование)
            print("\n1. Creating entities table...")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS entities (
                    entity_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    entity_type TEXT NOT NULL CHECK(entity_type IN ('employee', 'equipment')),
                    tag_id TEXT UNIQUE,
                    department TEXT,
                    role TEXT,
                    equipment_type TEXT,
                    is_active INTEGER DEFAULT 1,
                    metadata TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            print("   ✅ entities table created")
            
            # Индекс для быстрого поиска по tag_id
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_entities_tag_id ON entities(tag_id)
            """)
            
            # 2. geofences (геозоны)
            print("\n2. Creating geofences table...")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS geofences (
                    geofence_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    zone_type TEXT NOT NULL CHECK(zone_type IN ('restricted', 'danger', 'safe', 'work_area', 'parking', 'other')),
                    description TEXT,
                    shape TEXT NOT NULL CHECK(shape IN ('rectangle', 'circle', 'polygon')),
                    coordinates TEXT NOT NULL,  -- JSON
                    buffer_meters REAL DEFAULT 0.0,
                    is_active INTEGER DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            print("   ✅ geofences table created")
            
            # 3. rules (правила доступа)
            print("\n3. Creating rules table...")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS rules (
                    rule_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    description TEXT,
                    entity_type TEXT NOT NULL CHECK(entity_type IN ('employee', 'equipment', 'all')),
                    entity_id TEXT,
                    role_required TEXT,
                    geofence_id TEXT NOT NULL,
                    action TEXT NOT NULL CHECK(action IN ('allow', 'deny', 'alert')),
                    schedule TEXT,  -- JSON
                    severity TEXT NOT NULL CHECK(severity IN ('low', 'medium', 'high', 'critical')),
                    is_active INTEGER DEFAULT 1,
                    metadata TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (geofence_id) REFERENCES geofences(geofence_id)
                )
            """)
            print("   ✅ rules table created")
            
            # Индексы для быстрого поиска правил
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_rules_entity ON rules(entity_type, entity_id, is_active)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_rules_geofence ON rules(geofence_id, is_active)
            """)
            
            # 4. violations (нарушения)
            print("\n4. Creating violations table...")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS violations (
                    violation_id TEXT PRIMARY KEY,
                    rule_id TEXT NOT NULL,
                    rule_name TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    entity_name TEXT,
                    entity_type TEXT,
                    geofence_id TEXT NOT NULL,
                    geofence_name TEXT,
                    position TEXT NOT NULL,  -- JSON
                    severity TEXT NOT NULL CHECK(severity IN ('low', 'medium', 'high', 'critical')),
                    description TEXT,
                    timestamp TIMESTAMP NOT NULL,
                    acknowledged INTEGER DEFAULT 0,
                    acknowledged_by TEXT,
                    acknowledged_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (rule_id) REFERENCES rules(rule_id),
                    FOREIGN KEY (entity_id) REFERENCES entities(entity_id),
                    FOREIGN KEY (geofence_id) REFERENCES geofences(geofence_id)
                )
            """)
            print("   ✅ violations table created")
            
            # Индексы для быстрого поиска нарушений
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_violations_entity_time ON violations(entity_id, timestamp)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_violations_severity ON violations(severity, acknowledged)
            """)
            
            # Проверка созданных таблиц
            print("\n5. Checking created tables...")
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()
            print(f"   📋 Tables in DB: {[row[0] for row in tables]}")
            
            # Добавление демо-данных если таблицы пустые
            print("\n6. Adding demo data if needed...")
            
            # Проверяем есть ли уже демо-данные в entities
            cursor = conn.execute("SELECT COUNT(*) FROM entities")
            if cursor.fetchone()[0] == 0:
                print("   Adding demo entities...")
                demo_entities = [
                    ('emp-001', 'Иванов Иван Иванович', 'employee', 'tag-employee-123', 'IT отдел', 'инженер', None, 1, json.dumps({"employee_id": 1001})),
                    ('emp-002', 'Петров Петр Петрович', 'employee', 'tag-employee-456', 'Бухгалтерия', 'бухгалтер', None, 1, json.dumps({"employee_id": 1002})),
                    ('eq-001', 'Станок ЧПУ №1', 'equipment', 'tag-equipment-001', None, None, 'metal_cutting', 1, json.dumps({"model": "CNC-3000", "serial": "SN12345"})),
                ]
                conn.executemany(
                    "INSERT INTO entities (entity_id, name, entity_type, tag_id, department, role, equipment_type, is_active, metadata) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    demo_entities
                )
                print("   ✅ Demo entities added")
            
            # Проверяем есть ли уже демо-данные в geofences
            cursor = conn.execute("SELECT COUNT(*) FROM geofences")
            if cursor.fetchone()[0] == 0:
                print("   Adding demo geofences...")
                demo_geofences = [
                    (
                        str(uuid4()),
                        'Серверная комната',
                        'restricted',
                        'Запрещенная зона для посторонних',
                        'rectangle',
                        json.dumps({
                            "min_x": 10.0, "max_x": 15.0,
                            "min_y": 5.0, "max_y": 10.0,
                            "min_z": 0.0, "max_z": 3.0
                        }),
                        0.5
                    ),
                    (
                        str(uuid4()),
                        'Опасная зона станка',
                        'danger',
                        'Опасная зона вокруг станка',
                        'circle',
                        json.dumps({
                            "center_x": 25.0, "center_y": 30.0, 
                            "radius": 3.0
                        }),
                        0.0
                    ),
                    (
                        str(uuid4()),
                        'Зона отдыха',
                        'safe',
                        'Зона отдыха сотрудников',
                        'rectangle',
                        json.dumps({
                            "min_x": 30.0, "max_x": 40.0,
                            "min_y": 20.0, "max_y": 25.0,
                            "min_z": 0.0, "max_z": 3.0
                        }),
                        0.0
                    ),
                ]
                conn.executemany(
                    "INSERT INTO geofences (geofence_id, name, zone_type, description, shape, coordinates, buffer_meters, is_active) VALUES (?, ?, ?, ?, ?, ?, ?, 1)",
                    demo_geofences
                )
                print("   ✅ Demo geofences added")
                
                # Сохраняем ID геозон для создания правил
                cursor = conn.execute("SELECT geofence_id, zone_type FROM geofences")
                geofence_ids = cursor.fetchall()
                
                if geofence_ids:
                    print("   Adding demo rules...")
                    # Получаем первый geofence_id для restricted зоны
                    restricted_id = None
                    danger_id = None
                    for gid, zone_type in geofence_ids:
                        if zone_type == 'restricted':
                            restricted_id = gid
                        elif zone_type == 'danger':
                            danger_id = gid
                    
                    demo_rules = []
                    if restricted_id:
                        demo_rules.append((
                            str(uuid4()),
                            'Только IT в серверную',
                            'Доступ в серверную комнату только для сотрудников IT отдела',
                            'employee',
                            'emp-001',  # Только Иванов
                            None,
                            restricted_id,
                            'allow',
                            json.dumps({"days_of_week": [0, 1, 2, 3, 4, 5, 6], "start_time": "00:00", "end_time": "23:59"}),
                            'high',
                            1,
                            json.dumps({"auto_generated": True})
                        ))
                    
                    if danger_id:
                        demo_rules.append((
                            str(uuid4()),
                            'Опасная зона - всем запрещено',
                            'Никто не может входить в опасную зону станка',
                            'all',
                            None,
                            None,
                            danger_id,
                            'deny',
                            json.dumps({"days_of_week": [0, 1, 2, 3, 4, 5, 6], "start_time": "00:00", "end_time": "23:59"}),
                            'critical',
                            1,
                            json.dumps({"auto_generated": True})
                        ))
                    
                    if demo_rules:
                        conn.executemany(
                            """INSERT INTO rules (rule_id, name, description, entity_type, entity_id, role_required, 
                                                  geofence_id, action, schedule, severity, is_active, metadata) 
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                            demo_rules
                        )
                        print("   ✅ Demo rules added")
            
            conn.commit()
            print("\n✅ COMMIT successful")
            
    except Exception as e:
        print(f"\n❌ CRITICAL ERROR in init_db: {e}")
        import traceback
        traceback.print_exc()
        return
    
    print("\n" + "=" * 50)
    print("🟢 ACCESS CONTROL SERVICE INIT_DB COMPLETED")
    print("=" * 50)

# ==================== CRUD для entities ====================
def create_entity(entity_data: dict) -> Dict[str, Any]:
    """Создание новой сущности"""
    with get_db() as conn:
        # Подготавливаем metadata для JSON
        metadata = json.dumps(entity_data.get('metadata')) if entity_data.get('metadata') else None
        
        conn.execute("""
            INSERT INTO entities 
            (entity_id, name, entity_type, tag_id, department, role, equipment_type, is_active, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            entity_data['entity_id'],
            entity_data['name'],
            entity_data['entity_type'],
            entity_data.get('tag_id'),
            entity_data.get('department'),
            entity_data.get('role'),
            entity_data.get('equipment_type'),
            entity_data.get('is_active', True),
            metadata
        ))
        conn.commit()
        
        # Получаем созданную сущность
        cursor = conn.execute("SELECT * FROM entities WHERE entity_id = ?", (entity_data['entity_id'],))
        return dict(cursor.fetchone())

def get_all_entities(entity_type: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
    """Получение всех сущностей с фильтрацией по типу"""
    with get_db() as conn:
        query = "SELECT * FROM entities"
        params = []
        
        if entity_type and entity_type != 'all':
            query += " WHERE entity_type = ?"
            params.append(entity_type)
        
        query += " ORDER BY entity_id LIMIT ?"
        params.append(limit)
        
        cursor = conn.execute(query, params)
        rows = cursor.fetchall()
        
        result = []
        for row in rows:
            entity = dict(row)
            # Парсим JSON метаданные если они есть
            if entity.get('metadata'):
                entity['metadata'] = json.loads(entity['metadata'])
            result.append(entity)
        
        return result

def get_entity_by_id(entity_id: str) -> Optional[Dict[str, Any]]:
    """Получение сущности по ID"""
    with get_db() as conn:
        cursor = conn.execute("SELECT * FROM entities WHERE entity_id = ?", (entity_id,))
        row = cursor.fetchone()
        
        if row:
            entity = dict(row)
            if entity.get('metadata'):
                entity['metadata'] = json.loads(entity['metadata'])
            return entity
        return None

def get_entity_by_tag_id(tag_id: str) -> Optional[Dict[str, Any]]:
    """Получение сущности по привязанной метке"""
    with get_db() as conn:
        cursor = conn.execute("SELECT * FROM entities WHERE tag_id = ?", (tag_id,))
        row = cursor.fetchone()
        
        if row:
            entity = dict(row)
            if entity.get('metadata'):
                entity['metadata'] = json.loads(entity['metadata'])
            return entity
        return None

def update_entity(entity_id: str, update_data: dict) -> Optional[Dict[str, Any]]:
    """Обновление сущности"""
    with get_db() as conn:
        # Проверяем существует ли сущность
        if not get_entity_by_id(entity_id):
            return None
        
        # Подготавливаем обновляемые поля
        fields = []
        params = []
        
        for field in ['name', 'tag_id', 'department', 'role', 'is_active']:
            if field in update_data:
                fields.append(f"{field} = ?")
                params.append(update_data[field])
        
        if 'metadata' in update_data:
            fields.append("metadata = ?")
            params.append(json.dumps(update_data['metadata']))
        
        # Добавляем updated_at
        fields.append("updated_at = CURRENT_TIMESTAMP")
        
        if fields:
            params.append(entity_id)
            query = f"UPDATE entities SET {', '.join(fields)} WHERE entity_id = ?"
            conn.execute(query, params)
            conn.commit()
        
        # Возвращаем обновленную сущность
        return get_entity_by_id(entity_id)

def delete_entity(entity_id: str) -> bool:
    """Удаление сущности"""
    with get_db() as conn:
        cursor = conn.execute("DELETE FROM entities WHERE entity_id = ?", (entity_id,))
        conn.commit()
        return cursor.rowcount > 0

# ==================== CRUD для geofences ====================
def create_geofence(geofence_data: dict) -> Dict[str, Any]:
    """Создание новой геозоны"""
    with get_db() as conn:
        geofence_id = str(uuid4())
        
        conn.execute("""
            INSERT INTO geofences 
            (geofence_id, name, zone_type, description, shape, coordinates, buffer_meters, is_active)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            geofence_id,
            geofence_data['name'],
            geofence_data['zone_type'],
            geofence_data.get('description'),
            geofence_data['shape'],
            json.dumps(geofence_data['coordinates']),
            geofence_data.get('buffer_meters', 0.0),
            geofence_data.get('is_active', True)
        ))
        conn.commit()
        
        # Получаем созданную геозону
        return get_geofence_by_id(geofence_id)

def get_all_geofences() -> List[Dict[str, Any]]:
    """Получение всех геозон"""
    with get_db() as conn:
        cursor = conn.execute("SELECT * FROM geofences ORDER BY name")
        rows = cursor.fetchall()
        
        result = []
        for row in rows:
            geofence = dict(row)
            # Парсим JSON координаты
            if geofence.get('coordinates'):
                geofence['coordinates'] = json.loads(geofence['coordinates'])
            result.append(geofence)
        
        return result

def get_geofence_by_id(geofence_id: str) -> Optional[Dict[str, Any]]:
    """Получение геозоны по ID"""
    with get_db() as conn:
        cursor = conn.execute("SELECT * FROM geofences WHERE geofence_id = ?", (geofence_id,))
        row = cursor.fetchone()
        
        if row:
            geofence = dict(row)
            if geofence.get('coordinates'):
                geofence['coordinates'] = json.loads(geofence['coordinates'])
            return geofence
        return None

def update_geofence(geofence_id: str, geofence_data: dict) -> Optional[Dict[str, Any]]:
    """Обновление геозоны"""
    with get_db() as conn:
        # Проверяем существует ли геозона
        if not get_geofence_by_id(geofence_id):
            return None
        
        # Подготавливаем обновляемые поля
        fields = []
        params = []
        
        for field in ['name', 'zone_type', 'description', 'shape', 'buffer_meters', 'is_active']:
            if field in geofence_data:
                fields.append(f"{field} = ?")
                params.append(geofence_data[field])
        
        if 'coordinates' in geofence_data:
            fields.append("coordinates = ?")
            params.append(json.dumps(geofence_data['coordinates']))
        
        # Добавляем updated_at
        fields.append("updated_at = CURRENT_TIMESTAMP")
        
        if fields:
            params.append(geofence_id)
            query = f"UPDATE geofences SET {', '.join(fields)} WHERE geofence_id = ?"
            conn.execute(query, params)
            conn.commit()
        
        # Возвращаем обновленную геозону
        return get_geofence_by_id(geofence_id)

def delete_geofence(geofence_id: str) -> bool:
    """Удаление геозоны"""
    with get_db() as conn:
        cursor = conn.execute("DELETE FROM geofences WHERE geofence_id = ?", (geofence_id,))
        conn.commit()
        return cursor.rowcount > 0

def check_point_in_geofences(x: float, y: float, z: float = 0.0, 
                           geofence_ids: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """Проверка нахождения точки в геозонах"""
    with get_db() as conn:
        # Получаем все активные геозоны или конкретные по ID
        query = "SELECT * FROM geofences WHERE is_active = 1"
        params = []
        
        if geofence_ids:
            placeholders = ','.join(['?'] * len(geofence_ids))
            query += f" AND geofence_id IN ({placeholders})"
            params.extend(geofence_ids)
        
        cursor = conn.execute(query, params)
        geofences = cursor.fetchall()
        
        result = []
        for row in geofences:
            geofence = dict(row)
            if geofence.get('coordinates'):
                geofence['coordinates'] = json.loads(geofence['coordinates'])
            
            # Проверка нахождения точки в геозоне (упрощенная логика)
            is_inside = False
            coordinates = geofence['coordinates']
            
            if geofence['shape'] == 'rectangle':
                min_x = coordinates.get('min_x', 0) - geofence['buffer_meters']
                max_x = coordinates.get('max_x', 0) + geofence['buffer_meters']
                min_y = coordinates.get('min_y', 0) - geofence['buffer_meters']
                max_y = coordinates.get('max_y', 0) + geofence['buffer_meters']
                min_z = coordinates.get('min_z', 0) - geofence['buffer_meters']
                max_z = coordinates.get('max_z', 3) + geofence['buffer_meters']
                
                is_inside = (min_x <= x <= max_x and 
                           min_y <= y <= max_y and 
                           min_z <= z <= max_z)
            
            elif geofence['shape'] == 'circle':
                center_x = coordinates.get('center_x', 0)
                center_y = coordinates.get('center_y', 0)
                radius = coordinates.get('radius', 0) + geofence['buffer_meters']
                
                distance = ((x - center_x) ** 2 + (y - center_y) ** 2) ** 0.5
                is_inside = distance <= radius
            
            # Для polygon потребуется более сложная логика
            
            if is_inside:
                result.append({
                    'geofence_id': geofence['geofence_id'],
                    'geofence_name': geofence['name'],
                    'zone_type': geofence['zone_type'],
                    'is_inside': True
                })
        
        return result

# ==================== CRUD для rules ====================
def create_rule(rule_data: dict) -> Dict[str, Any]:
    """Создание нового правила"""
    with get_db() as conn:
        rule_id = str(uuid4())
        
        conn.execute("""
            INSERT INTO rules 
            (rule_id, name, description, entity_type, entity_id, role_required, 
             geofence_id, action, schedule, severity, is_active, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            rule_id,
            rule_data['name'],
            rule_data.get('description'),
            rule_data['entity_type'],
            rule_data.get('entity_id'),
            rule_data.get('role_required'),
            str(rule_data['geofence_id']),
            rule_data['action'],
            json.dumps(rule_data.get('schedule')) if rule_data.get('schedule') else None,
            rule_data.get('severity', 'medium'),
            rule_data.get('is_active', True),
            json.dumps(rule_data.get('metadata')) if rule_data.get('metadata') else None
        ))
        conn.commit()
        
        # Получаем созданное правило
        return get_rule_by_id(rule_id)

def get_all_rules(is_active: Optional[bool] = None) -> List[Dict[str, Any]]:
    """Получение всех правил с фильтрацией по активности"""
    with get_db() as conn:
        query = "SELECT * FROM rules"
        params = []
        
        if is_active is not None:
            query += " WHERE is_active = ?"
            params.append(1 if is_active else 0)
        
        query += " ORDER BY created_at DESC"
        
        cursor = conn.execute(query, params)
        rows = cursor.fetchall()
        
        result = []
        for row in rows:
            rule = dict(row)
            # Парсим JSON поля
            if rule.get('schedule'):
                rule['schedule'] = json.loads(rule['schedule'])
            if rule.get('metadata'):
                rule['metadata'] = json.loads(rule['metadata'])
            result.append(rule)
        
        return result

def get_rule_by_id(rule_id: str) -> Optional[Dict[str, Any]]:
    """Получение правила по ID"""
    with get_db() as conn:
        cursor = conn.execute("SELECT * FROM rules WHERE rule_id = ?", (rule_id,))
        row = cursor.fetchone()
        
        if row:
            rule = dict(row)
            # Парсим JSON поля
            if rule.get('schedule'):
                rule['schedule'] = json.loads(rule['schedule'])
            if rule.get('metadata'):
                rule['metadata'] = json.loads(rule['metadata'])
            return rule
        return None

def update_rule(rule_id: str, update_data: dict) -> Optional[Dict[str, Any]]:
    """Обновление правила"""
    with get_db() as conn:
        # Проверяем существует ли правило
        if not get_rule_by_id(rule_id):
            return None
        
        # Подготавливаем обновляемые поля
        fields = []
        params = []
        
        for field in ['name', 'description', 'is_active', 'severity']:
            if field in update_data:
                fields.append(f"{field} = ?")
                params.append(update_data[field])
        
        if 'schedule' in update_data:
            fields.append("schedule = ?")
            params.append(json.dumps(update_data['schedule']))
        
        if 'metadata' in update_data:
            fields.append("metadata = ?")
            params.append(json.dumps(update_data['metadata']))
        
        # Добавляем updated_at
        fields.append("updated_at = CURRENT_TIMESTAMP")
        
        if fields:
            params.append(rule_id)
            query = f"UPDATE rules SET {', '.join(fields)} WHERE rule_id = ?"
            conn.execute(query, params)
            conn.commit()
        
        # Возвращаем обновленное правило
        return get_rule_by_id(rule_id)

def delete_rule(rule_id: str) -> bool:
    """Удаление правила"""
    with get_db() as conn:
        cursor = conn.execute("DELETE FROM rules WHERE rule_id = ?", (rule_id,))
        conn.commit()
        return cursor.rowcount > 0

def get_applicable_rules(entity_type: str, entity_id: Optional[str] = None, 
                        role: Optional[str] = None) -> List[Dict[str, Any]]:
    """Получение правил, применимых к конкретной сущности"""
    with get_db() as conn:
        # Правила, которые применяются ко всем (entity_type = 'all')
        query_all = """
            SELECT r.*, g.name as geofence_name, g.zone_type 
            FROM rules r 
            JOIN geofences g ON r.geofence_id = g.geofence_id 
            WHERE r.is_active = 1 AND g.is_active = 1 
            AND r.entity_type = 'all'
        """
        
        # Правила для конкретного типа сущности
        query_type = """
            SELECT r.*, g.name as geofence_name, g.zone_type 
            FROM rules r 
            JOIN geofences g ON r.geofence_id = g.geofence_id 
            WHERE r.is_active = 1 AND g.is_active = 1 
            AND r.entity_type = ?
        """
        
        # Правила для конкретной сущности
        query_entity = """
            SELECT r.*, g.name as geofence_name, g.zone_type 
            FROM rules r 
            JOIN geofences g ON r.geofence_id = g.geofence_id 
            WHERE r.is_active = 1 AND g.is_active = 1 
            AND r.entity_type = ? AND r.entity_id = ?
        """
        
        result = []
        
        # 1. Правила для всех
        cursor = conn.execute(query_all)
        result.extend([dict(row) for row in cursor.fetchall()])
        
        # 2. Правила для типа сущности
        cursor = conn.execute(query_type, (entity_type,))
        result.extend([dict(row) for row in cursor.fetchall()])
        
        # 3. Правила для конкретной сущности
        if entity_id:
            cursor = conn.execute(query_entity, (entity_type, entity_id))
            result.extend([dict(row) for row in cursor.fetchall()])
        
        # Парсим JSON поля и фильтруем по роли если нужно
        filtered_result = []
        for rule in result:
            if rule.get('schedule'):
                rule['schedule'] = json.loads(rule['schedule'])
            if rule.get('metadata'):
                rule['metadata'] = json.loads(rule['metadata'])
            
            # Проверка требования к роли
            if rule.get('role_required') and role != rule['role_required']:
                continue
            
            filtered_result.append(rule)
        
        return filtered_result

# ==================== CRUD для violations ====================
def create_violation(violation_data: dict) -> Dict[str, Any]:
    """Создание записи о нарушении"""
    with get_db() as conn:
        violation_id = str(uuid4())
        
        conn.execute("""
            INSERT INTO violations 
            (violation_id, rule_id, rule_name, entity_id, entity_name, entity_type,
             geofence_id, geofence_name, position, severity, description, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            violation_id,
            str(violation_data['rule_id']),
            violation_data['rule_name'],
            violation_data['entity_id'],
            violation_data.get('entity_name'),
            violation_data.get('entity_type'),
            str(violation_data['geofence_id']),
            violation_data.get('geofence_name'),
            json.dumps(violation_data['position']),
            violation_data['severity'],
            violation_data.get('description'),
            violation_data['timestamp']
        ))
        conn.commit()
        
        # Получаем созданное нарушение
        return get_violation_by_id(violation_id)

def get_violations(start_time: Optional[str] = None, end_time: Optional[str] = None,
                  entity_id: Optional[str] = None, severity: Optional[str] = None,
                  limit: int = 100) -> List[Dict[str, Any]]:
    """Получение истории нарушений с фильтрами"""
    with get_db() as conn:
        query = "SELECT * FROM violations WHERE 1=1"
        params = []
        
        if start_time:
            query += " AND timestamp >= ?"
            params.append(start_time)
        
        if end_time:
            query += " AND timestamp <= ?"
            params.append(end_time)
        
        if entity_id:
            query += " AND entity_id = ?"
            params.append(entity_id)
        
        if severity:
            query += " AND severity = ?"
            params.append(severity)
        
        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)
        
        cursor = conn.execute(query, params)
        rows = cursor.fetchall()
        
        result = []
        for row in rows:
            violation = dict(row)
            # Парсим JSON поле position
            if violation.get('position'):
                violation['position'] = json.loads(violation['position'])
            result.append(violation)
        
        return result

def get_violation_by_id(violation_id: str) -> Optional[Dict[str, Any]]:
    """Получение нарушения по ID"""
    with get_db() as conn:
        cursor = conn.execute("SELECT * FROM violations WHERE violation_id = ?", (violation_id,))
        row = cursor.fetchone()
        
        if row:
            violation = dict(row)
            if violation.get('position'):
                violation['position'] = json.loads(violation['position'])
            return violation
        return None

def acknowledge_violation(violation_id: str, acknowledged_by: str) -> bool:
    """Подтверждение нарушения оператором"""
    with get_db() as conn:
        cursor = conn.execute("""
            UPDATE violations 
            SET acknowledged = 1, acknowledged_by = ?, acknowledged_at = CURRENT_TIMESTAMP 
            WHERE violation_id = ?
        """, (acknowledged_by, violation_id))
        conn.commit()
        return cursor.rowcount > 0
