#!/bin/bash
set -e

# Kafka 및 Postgres 호스트/포트 설정
# docker-compose에서 환경변수로 오버라이드 가능
PG_HOST="${POSTGRES_HOST:-postgres}"
PG_PORT="${POSTGRES_PORT:-5432}"
KAFKA_HOST="${KAFKA_HOST:-kafka}"
KAFKA_PORT="${KAFKA_PORT:-19092}"

echo "⏳ [Entrypoint] 서비스 대기 중..."

# Postgres 대기
while ! nc -z "$PG_HOST" "$PG_PORT"; do
  echo "   ...Waiting for Postgres ($PG_HOST:$PG_PORT)"
  sleep 2
done
echo "✅ Postgres 연결 가능!"

# Kafka 대기
while ! nc -z "$KAFKA_HOST" "$KAFKA_PORT"; do
  echo "   ...Waiting for Kafka ($KAFKA_HOST:$KAFKA_PORT)"
  sleep 2
done
echo "✅ Kafka 연결 가능!"

echo "🚀 [Start] 봇 프로세스 시작: $@"
exec "$@"
