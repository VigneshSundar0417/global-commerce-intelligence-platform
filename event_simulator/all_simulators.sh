#!/bin/bash

# Navigate to the simulator directory
cd "$(dirname "$0")"

echo "Starting ALL event simulators..."

# Start each simulator in the background
python3 generate_orders.py & 
echo "✓ Orders simulator started"

python3 generate_payments.py &
echo "✓ Payments simulator started"

python3 generate_inventory.py &
echo "✓ Inventory simulator started"

python3 generate_shipments.py &
echo "✓ Shipments simulator started"

python3 generate_customer_events.py &
echo "✓ Customer events simulator started"

echo ""
echo "All simulators are now running in the background."
echo "Use 'ps -ef | grep generate_' to verify."
echo "Use 'killall python3' to stop them."