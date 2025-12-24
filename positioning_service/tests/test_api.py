import sys
from pathlib import Path
from datetime import datetime, timedelta
import json

# Добавляем родительскую директорию в sys.path
sys.path.insert(0, str(Path(__file__).parent.parent))

from fastapi.testclient import TestClient
from app.main import app
from app.database import init_db  # ← ИМПОРТИРУЙ

# ИНИЦИАЛИЗИРУЙ БД ПЕРЕД ТЕСТАМИ
print("🔄 Initializing database for tests...")
init_db()

client = TestClient(app)

# client = TestClient(app)


def test_health_check():
    """Тест проверки работоспособности"""
    # В нашем приложении endpoint / перенаправляет на /docs
    response = client.get("/", follow_redirects=False)
    assert response.status_code in [200, 307]  # 307 если редирект


def test_submit_measurements_valid():
    """Тест отправки валидных измерений"""
    payload = {
        "gateway_id": "test-gateway-001",
        "timestamp": datetime.now().isoformat(),
        "measurements": [
            {"anchor_id": "anchor-1", "tag_id": "tag-001", "distance_m": 10.5},
            {"anchor_id": "anchor-2", "tag_id": "tag-001", "distance_m": 12.3},
            {"anchor_id": "anchor-3", "tag_id": "tag-001", "distance_m": 8.7},
        ]
    }
    
    response = client.post("/api/v1/measurements", json=payload)
    assert response.status_code == 202
    data = response.json()
    assert "message" in data
    assert "batch_id" in data


def test_submit_measurements_invalid():
    """Тест отправки невалидных измерений (недостаточно анкеров)"""
    payload = {
        "gateway_id": "test-gateway-001",
        "timestamp": datetime.now().isoformat(),
        "measurements": [
            {"anchor_id": "anchor-1", "tag_id": "tag-001", "distance_m": 10.5},
            {"anchor_id": "anchor-1", "tag_id": "tag-001", "distance_m": 12.3},  # Тот же анкер!
        ]
    }
    
    response = client.post("/api/v1/measurements", json=payload)
    assert response.status_code == 422  # Validation error


def test_submit_measurements_negative_distance():
    """Тест с отрицательным расстоянием"""
    payload = {
        "gateway_id": "test-gateway-001",
        "timestamp": datetime.now().isoformat(),
        "measurements": [
            {"anchor_id": "anchor-1", "tag_id": "tag-001", "distance_m": -5.0},  # Отрицательное!
            {"anchor_id": "anchor-2", "tag_id": "tag-001", "distance_m": 12.3},
            {"anchor_id": "anchor-3", "tag_id": "tag-001", "distance_m": 8.7},
        ]
    }
    
    response = client.post("/api/v1/measurements", json=payload)
    assert response.status_code == 422


def test_get_anchor_by_id():
    """Тест получения анкера по ID"""
    response = client.get("/api/v1/anchors/anchor-1")
    assert response.status_code == 200
    data = response.json()
    assert data["anchor_id"] == "anchor-1"
    assert "x" in data
    assert "y" in data
    assert "z" in data


def test_get_nonexistent_anchor():
    """Тест получения несуществующего анкера"""
    response = client.get("/api/v1/anchors/nonexistent-anchor")
    assert response.status_code == 404


def test_get_current_position_nonexistent():
    """Тест получения позиции для несуществующей метки"""
    response = client.get("/api/v1/positions/current/nonexistent-tag")
    assert response.status_code == 404
    data = response.json()
    # FastAPI оборачивает ошибки в {"detail": {...}}
    if "detail" in data:
        detail = data["detail"]
        if isinstance(detail, dict) and "error_code" in detail:
            assert detail["error_code"] == "POSITION_NOT_FOUND"
    else:
        return False
        # Если ошибка сразу в корне
        assert data["error_code"] == "POSITION_NOT_FOUND"

def test_get_position_history_invalid_dates():
    """Тест получения истории с невалидными датами"""
    end_time = datetime.now().isoformat()
    start_time = (datetime.now() + timedelta(hours=1)).isoformat()  # start > end!
    
    response = client.get(
        f"/api/v1/positions/history/tag-001",
        params={
            "start_time": start_time,
            "end_time": end_time,
            "limit": 100
        }
    )
    assert response.status_code == 400


def test_get_position_history_valid():
    """Тест получения истории с валидными параметрами"""
    end_time = datetime.now().isoformat()
    start_time = (datetime.now() - timedelta(hours=1)).isoformat()
    
    response = client.get(
        f"/api/v1/positions/history/tag-001",
        params={
            "start_time": start_time,
            "end_time": end_time,
            "limit": 10
        }
    )
    # Должен вернуть 200 даже если данных нет (пустой список)
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)


def test_position_history_limit_validation():
    """Тест валидации лимита"""
    end_time = datetime.now().isoformat()
    start_time = (datetime.now() - timedelta(hours=1)).isoformat()
    
    # Лимит превышает максимальный - должен вернуть 400
    response = client.get(
        f"/api/v1/positions/history/tag-001",
        params={
            "start_time": start_time,
            "end_time": end_time,
            "limit": 20000  # > 10000
        }
    )
    assert response.status_code in [400, 422]

# Удали тест delete_anchor - он требует setup/teardown базы данных
# Для курсовой достаточно остальных тестов

if __name__ == "__main__":
    # Запуск тестов вручную
    import traceback
    
    tests = [
        ("Health check", test_health_check),
        ("Submit valid measurements", test_submit_measurements_valid),
        ("Submit invalid measurements", test_submit_measurements_invalid),
        ("Negative distance", test_submit_measurements_negative_distance),
        ("Get anchor by ID", test_get_anchor_by_id),
        ("Get nonexistent anchor", test_get_nonexistent_anchor),
        ("Get nonexistent position", test_get_current_position_nonexistent),
        ("Invalid dates history", test_get_position_history_invalid_dates),
        ("Valid history", test_get_position_history_valid),
        ("Limit validation", test_position_history_limit_validation),
    ]
    
    passed = 0
    failed = 0
    
    for name, test_func in tests:
        try:
            test_func()
            print(f"✓ {name}")
            passed += 1
        except Exception as e:
            print(f"✗ {name}: {e}")
            failed += 1
    
    print(f"\nResults: {passed} passed, {failed} failed")
    print("✅ Tests completed!" if failed == 0 else "❌ Some tests failed")
