"""
Notification Service — слушает PaymentCompleted и OrderCancelled.
На каждое событие логирует «Отправлено уведомление клиенту».
"""

import os
import json
import time
import logging

import pika

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/%2F")
EXCHANGE = "marketplace_events"
QUEUE_COMPLETED = "notification_service.payment_completed"
QUEUE_CANCELLED = "notification_service.order_cancelled"

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] [notification] %(message)s")
log = logging.getLogger("notification")


def _params() -> pika.URLParameters:
    p = pika.URLParameters(RABBITMQ_URL)
    p.heartbeat = 0
    p.blocked_connection_timeout = 30
    return p


def on_completed(ch, method, properties, body):
    try:
        ev = json.loads(body)
    except Exception as e:
        log.error("Bad PaymentCompleted: %s", e)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return
    log.info("Received PaymentCompleted orderId=%s transactionId=%s",
             ev.get("orderId"), ev.get("transactionId"))
    log.info("Отправлено уведомление клиенту: оплата по заказу %s прошла успешно",
             ev.get("orderId"))
    ch.basic_ack(delivery_tag=method.delivery_tag)


def on_cancelled(ch, method, properties, body):
    try:
        ev = json.loads(body)
    except Exception as e:
        log.error("Bad OrderCancelled: %s", e)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return
    log.info("Received OrderCancelled orderId=%s reason=%s",
             ev.get("orderId"), ev.get("reason"))
    log.info("Отправлено уведомление клиенту: заказ %s отменён (причина: %s)",
             ev.get("orderId"), ev.get("reason"))
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    while True:
        try:
            conn = pika.BlockingConnection(_params())
            ch = conn.channel()
            ch.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)

            ch.queue_declare(queue=QUEUE_COMPLETED, durable=True)
            ch.queue_bind(exchange=EXCHANGE, queue=QUEUE_COMPLETED,
                          routing_key="payment.completed")

            ch.queue_declare(queue=QUEUE_CANCELLED, durable=True)
            ch.queue_bind(exchange=EXCHANGE, queue=QUEUE_CANCELLED,
                          routing_key="order.cancelled")

            ch.basic_qos(prefetch_count=1)
            ch.basic_consume(queue=QUEUE_COMPLETED, on_message_callback=on_completed)
            ch.basic_consume(queue=QUEUE_CANCELLED, on_message_callback=on_cancelled)
            log.info("Notification consumer started (queues: %s, %s)",
                     QUEUE_COMPLETED, QUEUE_CANCELLED)
            ch.start_consuming()
        except Exception as e:
            log.warning("Connection error: %s; retry in 5s", e)
            time.sleep(5)


if __name__ == "__main__":
    main()