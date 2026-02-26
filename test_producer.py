from kafka import KafkaProducer
import json
from datetime import datetime

KAFKA_HOST = "keyword-broker-1:9093,keyword-broker-2:9093"
KAFKA_TOPIC = "keyword"

message = {
    "keyword": "TOEIC",
    "params": "day",
    "start_time": datetime.now().isoformat(),
    "platforms": [
        "facebook"
    ]
}

# message = {
#  	"keyword_link": "https://www.facebook.com/reel/4166095473638250/",
#  	"params": "day",
#  	"start_time": datetime.now().isoformat(),
#  	"platforms": [
#  		"facebook"
#  	]
# }

producer = KafkaProducer(
    bootstrap_servers=KAFKA_HOST.split(","),
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    key_serializer=lambda k: str(k).encode("utf-8") if k else None,
    acks="all",            
    retries=3,               
    linger_ms=10            
)

try:
    producer.send(KAFKA_TOPIC, value=message)
    producer.flush()
    print(f"✅ Đã gửi message vào topic '{KAFKA_TOPIC}':")
    print(json.dumps(message, ensure_ascii=False, indent=2))
except Exception as e:
    print(f"❌ Lỗi khi gửi message: {e}")
finally:
    producer.close()
