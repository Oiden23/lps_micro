import asyncio
import aiohttp
import random
import time
import uuid
from datetime import datetime, timezone
from typing import List, Dict, Tuple
import numpy as np
import sys

class TagSimulator:
    def __init__(self, base_url: str = "http://0.0.0.0:8082"):
        self.base_url = base_url
        self.tags = []
        self.anchors = []
        self.positions = {}  # tag_id -> (x, y, z)
        
    async def initialize(self):
        """Инициализация - получение анкеров и создание меток"""
        # Получаем анкеры с сервера
        print("Получение списка анкеров...")
        self.anchors = await self.get_anchors_from_server()
        
        if len(self.anchors) < 3:
            print(f"Предупреждение: найдено только {len(self.anchors)} анкеров")
            # Если анкеров нет, создаем тестовые
            if len(self.anchors) == 0:
                print("Создание тестовых анкеров...")
                await self.create_test_anchors()
                self.anchors = await self.get_anchors_from_server()
        
        if len(self.anchors) < 3:
            raise ValueError(f"Нужно минимум 3 анкера для трилатерации, а найдено {len(self.anchors)}")
        
        # Создаем 10 меток со случайными начальными позициями
        for i in range(10):
            tag_id = f"tag-employee-{i+100}"
            self.tags.append(tag_id)    
            # Начальная позиция в пределах комнаты 20x20 метров
            self.positions[tag_id] = (
                random.uniform(1, 19),
                random.uniform(1, 19),
                random.uniform(0.5, 2)  # Высота 0.5-2 метра (уровень пояса человека)
            )
        
        print(f"Инициализировано {len(self.tags)} меток")
        print(f"Используются анкеры: {[a['anchor_id'] for a in self.anchors[:3]]}")
        
    async def get_anchors_from_server(self) -> List[Dict]:
        """Получение списка анкеров с сервера"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{self.base_url}/api/v1/anchors") as response:
                    if response.status == 200:
                        anchors = await response.json()
                        print(f"Получено {len(anchors)} анкеров с сервера")
                        return anchors
                    else:
                        print(f"Ошибка при получении анкеров: {response.status}")
                        return []
        except Exception as e:
            print(f"Исключение при получении анкеров: {e}")
            return []
    
    async def create_test_anchors(self):
        """Создание тестовых анкеров если их нет на сервере"""
        test_anchors = [
            {
                "anchor_id": "anchor-1",
                "x": 0.0,
                "y": 0.0,
                "z": 2.5,
                "description": "Северо-западный угол",
                "is_active": True,
                "last_calibration": datetime.now(timezone.utc).isoformat()
            },
            {
                "anchor_id": "anchor-2",
                "x": 20.0,
                "y": 0.0,
                "z": 2.5,
                "description": "Северо-восточный угол",
                "is_active": True,
                "last_calibration": datetime.now(timezone.utc).isoformat()
            },
            {
                "anchor_id": "anchor-3",
                "x": 0.0,
                "y": 20.0,
                "z": 2.5,
                "description": "Юго-западный угол",
                "is_active": True,
                "last_calibration": datetime.now(timezone.utc).isoformat()
            },
            {
                "anchor_id": "anchor-4",
                "x": 20.0,
                "y": 20.0,
                "z": 3.0,
                "description": "Юго-восточный угол, потолок",
                "is_active": True,
                "last_calibration": datetime.now(timezone.utc).isoformat()
            }
        ]
        
        async with aiohttp.ClientSession() as session:
            for anchor in test_anchors:
                try:
                    async with session.post(f"{self.base_url}/api/v1/anchors", json=anchor) as response:
                        if response.status in [200, 201]:
                            print(f"Создан анкер: {anchor['anchor_id']}")
                        else:
                            print(f"Ошибка создания анкера {anchor['anchor_id']}: {response.status}")
                except Exception as e:
                    print(f"Исключение при создании анкера {anchor['anchor_id']}: {e}")
        
        # Даем время на обработку
        await asyncio.sleep(1)
    
    def calculate_distance(self, tag_pos: Tuple[float, float, float], 
                          anchor: Dict) -> float:
        """Вычисление расстояния от метки до анкера"""
        dx = tag_pos[0] - anchor['x']
        dy = tag_pos[1] - anchor['y']
        dz = tag_pos[2] - anchor['z']
        return np.sqrt(dx*dx + dy*dy + dz*dz)
    
    def update_position(self, tag_id: str):
        """Обновление позиции метки (небольшое случайное изменение)"""
        x, y, z = self.positions[tag_id]
        
        # Случайное смещение (имитация ходьбы человека)
        step_size = 0.3  # Максимальный шаг в метрах
        x += random.uniform(-step_size, step_size)
        y += random.uniform(-step_size, step_size)
        
        # Небольшое изменение высоты (имитация естественных колебаний)
        z += random.uniform(-0.05, 0.05)
        
        # Ограничиваем в пределах комнаты
        x = max(0.5, min(x, 19.5))
        y = max(0.5, min(y, 19.5))
        z = max(0.5, min(z, 3.0))  # Высота от пола до потолка
        
        self.positions[tag_id] = (x, y, z)
        return (x, y, z)
    
    def create_measurement_batch(self) -> Dict:
        """Создание пакета измерений для ВСЕХ меток одновременно"""
        measurements = []
        
        for tag_id in self.tags:
            # Обновляем позицию для каждой метки
            pos = self.update_position(tag_id)
            
            # Для каждой метки создаем измерения для случайных 3 анкеров
            # (имитируем реальную ситуацию, когда не все анкеры видят метку)
            available_anchors = random.sample(self.anchors, min(3, len(self.anchors)))
            
            for anchor in available_anchors:
                distance = self.calculate_distance(pos, anchor)
                # Добавляем реалистичную погрешность измерения (±0.2 м)
                distance_with_noise = distance + random.uniform(-0.2, 0.2)
                distance_with_noise = max(0.3, distance_with_noise)  # Минимальное расстояние
                
                measurements.append({
                    "anchor_id": anchor['anchor_id'],
                    "tag_id": tag_id,
                    "distance_m": round(distance_with_noise, 3)
                })
        
        return {
            "gateway_id": f"gateway-simulator-{random.randint(1, 5)}",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "measurements": measurements
        }
    
    async def send_measurements(self, session: aiohttp.ClientSession) -> bool:
        """Отправка пакета измерений"""
        try:
            batch = self.create_measurement_batch()
            
            print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Отправка пакета:")
            print(f"  Gateway: {batch['gateway_id']}")
            print(f"  Меток: {len(self.tags)}")
            print(f"  Измерений: {len(batch['measurements'])}")
            
            async with session.post(
                f"{self.base_url}/api/v1/measurements",
                json=batch
            ) as response:
                if response.status == 202:
                    result = await response.json()
                    print(f"  ✓ Принято в обработку, batch_id: {result.get('batch_id', 'N/A')}")
                    
                    # Выводим текущие позиции
                    print(f"  Текущие позиции:")
                    for tag_id in self.tags[:3]:  # Показываем только первые 3
                        x, y, z = self.positions[tag_id]
                        print(f"    {tag_id}: ({x:.2f}, {y:.2f}, {z:.2f})")
                    if len(self.tags) > 3:
                        print(f"    ... и еще {len(self.tags) - 3} меток")
                    
                    return True
                else:
                    error_text = await response.text()
                    print(f"  ✗ Ошибка {response.status}: {error_text[:100]}")
                    return False
                    
        except Exception as e:
            print(f"  ✗ Исключение: {str(e)}")
            return False
    
    async def verify_current_positions(self, session: aiohttp.ClientSession):
        """Проверка текущих позиций на сервере"""
        print("\nПроверка текущих позиций на сервере:")
        for tag_id in self.tags[:2]:  # Проверяем только первые 2 для примера
            try:
                async with session.get(f"{self.base_url}/api/v1/positions/current/{tag_id}") as response:
                    if response.status == 200:
                        pos = await response.json()
                        x, y, z = self.positions[tag_id]
                        print(f"  {tag_id}: сервер ({pos['x']:.2f}, {pos['y']:.2f}, {pos['z']:.2f}), "
                              f"симулятор ({x:.2f}, {y:.2f}, {z:.2f})")
                    elif response.status == 404:
                        print(f"  {tag_id}: позиция еще не вычислена")
                    else:
                        print(f"  {tag_id}: ошибка {response.status}")
            except Exception as e:
                print(f"  {tag_id}: исключение при проверке - {e}")
    
    async def run_simulation(self, delay: float = 2.0, verify_every: int = 10):
        """Запуск симуляции"""
        print("=" * 60)
        print("Запуск симуляции RTLS системы")
        print("=" * 60)
        print(f"Базовый URL: {self.base_url}")
        print(f"Интервал отправки: {delay} секунд")
        print(f"Меток: {len(self.tags)}")
        print(f"Анкеров: {len(self.anchors)}")
        print("=" * 60)
        
        iteration = 0
        async with aiohttp.ClientSession() as session:
            while True:
                iteration += 1
                print(f"\n{'='*40}")
                print(f"Итерация #{iteration}")
                print(f"{'='*40}")
                
                # Отправляем пакет измерений
                success = await self.send_measurements(session)
                
                # Периодически проверяем позиции на сервере
                if iteration % verify_every == 0:
                    await self.verify_current_positions(session)
                
                # Ждем перед следующей итерацией
                if iteration < 5:  # Первые 5 итераций быстрее
                    await asyncio.sleep(1)
                else:
                    print(f"Ожидание {delay} секунд до следующей отправки...")
                    await asyncio.sleep(delay)
    
    def print_current_positions(self):
        """Вывод текущих позиций всех меток"""
        print("\nТекущие позиции меток в симуляторе:")
        print("-" * 50)
        for tag_id in self.tags:
            x, y, z = self.positions[tag_id]
            print(f"{tag_id}: ({x:.2f}, {y:.2f}, {z:.2f})")


async def main():
    # Настройки
    BASE_URL = "http://0.0.0.0:8001"  # URL вашего микросервиса
    DELAY_BETWEEN_ITERATIONS = 2.0  # Пауза между итерациями в секундах
    
    # Создаем и инициализируем симулятор
    simulator = TagSimulator(base_url=BASE_URL)
    
    try:
        await simulator.initialize()
        simulator.print_current_positions()
        
        # Запускаем симуляцию
        await simulator.run_simulation(delay=DELAY_BETWEEN_ITERATIONS)
        
    except KeyboardInterrupt:
        print("\n\n" + "="*50)
        print("Симуляция остановлена пользователем")
        print("="*50)
        simulator.print_current_positions()
        print("\nПоследние позиции сохранены. Для продолжения запустите снова.")
    except Exception as e:
        print(f"\nОшибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    # Для Windows нужна специальная политика event loop
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    asyncio.run(main())
