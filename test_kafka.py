import os
import json
import time
from kafka import KafkaConsumer

# ==============================================================
# 1️⃣ Cấu hình từ biến môi trường (hoặc bạn có thể ghi cứng vào đây)
# ==============================================================
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "keyword")
KAFKA_HOST = os.getenv("KAFKA_HOST", "keyword-broker-1:9093,keyword-broker-2:9093")
GROUP_ID = os.getenv("KAFKA_GROUP", "facebook-group")

print(f"📡 Đang kết nối đến Kafka broker: {KAFKA_HOST}")
print(f"📦 Đang subscribe topic: {KAFKA_TOPIC}")

# ==============================================================
# 2️⃣ Tạo KafkaConsumer
# ==============================================================
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_HOST.split(","),
    auto_offset_reset="earliest",      # hoặc 'earliest' nếu muốn đọc lại từ đầu
    enable_auto_commit=False,        # không tự commit offset
    group_id=GROUP_ID,
    max_poll_records=1,
    max_poll_interval_ms=600000,
    session_timeout_ms=60000,
    value_deserializer=lambda m: m.decode("utf-8", errors="ignore"),  # decode UTF-8
)

# ==============================================================
# 3️⃣ Vòng lặp đọc message
# ==============================================================
try:
    print("🚀 Kafka consumer đã khởi động. Đang chờ message...\n")
    for message in consumer:
        print("=" * 80)
        print(f"🧩 Topic: {message.topic}")
        print(f"🧭 Partition: {message.partition}")
        print(f"📍 Offset: {message.offset}")
        print(f"⏰ Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(message.timestamp/1000))}")
        print(f"🔑 Key: {message.key}")
        
        try:
            data = json.loads(message.value)
            print(f"📦 JSON Value:\n{json.dumps(data, indent=2, ensure_ascii=False)}")
        except json.JSONDecodeError:
            print(f"📝 Raw Value: {message.value}")

        print("=" * 80 + "\n")

        # Nếu bạn muốn commit offset thủ công
        consumer.commit()
        print("✅ Offset đã commit.\n")

except KeyboardInterrupt:
    print("\n🛑 Dừng consumer theo yêu cầu người dùng.")
finally:
    consumer.close()
    print("🔚 Đã đóng kết nối Kafka.")
