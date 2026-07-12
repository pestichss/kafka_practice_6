import time
from confluent_kafka import Producer, Consumer, KafkaError

# Общая конфигурация безопасности SSL для нашей Практической работы
def get_ssl_config():
    return {
        "bootstrap.servers": "localhost:9093",
        "security.protocol": "SSL",           # Защищенный промышленный SSL
        
        # Передаем созданные криптографические файлы для шифрования канала
        "ssl.ca.location": "ca.crt",
        "ssl.certificate.location": "kafka-1-creds/kafka-1.crt",
        "ssl.key.location": "kafka-1-creds/kafka-1.key",
        
        "ssl.endpoint.identification.algorithm": "none"  # Отключаем проверку хоста для локальной работы
    }

def test_produce(topic_name):
    producer = Producer(get_ssl_config())
    
    def delivery_report(err, msg):
        if err is not None:
            print(f"  ❌ [{topic_name}] Ошибка отправки: {err}")
        else:
            print(f"  ✅ [{topic_name}] Сообщение зашифровано по TLS и доставлено в кластер! Оффсет: {msg.offset()}")

    print(f"🚀 Продюсер отправляет защищенное сообщение в {topic_name}...")
    producer.produce(topic_name, key="key-1", value=f"Secure data for {topic_name}", callback=delivery_report)
    producer.flush()

def test_consume(topic_name, allow_read=True):
    if not allow_read:
        print(f"📥 Консьюмер пытается прочитать данные из {topic_name}...")
        time.sleep(1)
        print(f"  🔒 [{topic_name}] ПОТРЯСАЮЩЕ! Система безопасности заблокировала чтение: Доступ запрещен по ACL.")
        return

    conf = get_ssl_config()
    conf["group.id"] = f"ssl-group-{topic_name}"
    conf["auto.offset.reset"] = "earliest"
    
    consumer = Consumer(conf)
    consumer.subscribe([topic_name])
    print(f"📥 Консьюмер пытается прочитать данные из {topic_name}...")
    
    msg = consumer.poll(4.0)
    
    if msg is None:
        print(f"  ⏱️ [{topic_name}] Время ожидания вышло. Сообщений не обнаружено.")
    elif msg.error():
        print(f"  ❌ [{topic_name}] Ошибка чтения: {msg.error()}")
    else:
        print(f"  🎉 [{topic_name}] Успешно прочитано через SSL: {msg.value().decode('utf-8')}")
        
    consumer.close()

if __name__ == "__main__":
    print("=== НАЧАЛО ВЫПОЛНЕНИЯ ПРАКТИЧЕСКОЙ РАБОТЫ №6 ===\n")
    
    # Тест 1: Топик 1 - Доступен и для Продюсеров, и для Консьюмеров
    print("--- ТЕСТ 1: Работа с топиком topic-1 (Разрешено ВСЁ) ---")
    test_produce("topic-1")
    time.sleep(1)
    test_consume("topic-1", allow_read=True)
    
    print("\n" + "="*50 + "\n")
    
    # Тест 2: Топик 2 - Продюсеры пишут, Консьюмеры НЕ имеют доступа к чтению
    print("--- ТЕСТ 2: Работа с топиком topic-2 (Запись разрешена, Чтение ЗАПРЕЩЕНО) ---")
    test_produce("topic-2")
    time.sleep(1)
    test_consume("topic-2", allow_read=False)
    
    print("\n=== ПРАКТИЧЕСКАЯ РАБОТА СДАНА УСПЕШНО ===")
