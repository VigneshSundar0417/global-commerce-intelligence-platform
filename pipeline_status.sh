#!/bin/bash

echo "📊 Pipeline Status"

echo ""
echo "🔍 Kafka Containers:"
docker ps | grep kafka

echo ""
echo "🔍 Spark Jobs:"
ps -ef | grep -E "bronze_all_streams|silver_all_tables|gold_all_tables" | grep -v grep

echo ""
echo "🔍 Simulators:"
ps -ef | grep generate_ | grep -v grep

echo ""
echo "📁 Delta Tables (Bronze):"
ls -R data/bronze 2>/dev/null | head

echo ""
echo "📁 Delta Tables (Silver):"
ls -R data/silver 2>/dev/null | head

echo ""
echo "📁 Delta Tables (Gold):"
ls -R data/gold 2>/dev/null | head