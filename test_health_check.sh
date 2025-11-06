#!/bin/bash
# Test script for worker health checking

set -e

# Pick docker compose command (Docker Desktop uses `docker compose`)
if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
  COMPOSE="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE="docker-compose"
else
  echo "ERROR: Neither 'docker compose' nor 'docker-compose' found. Install Docker Desktop and try again."
  exit 1
fi

echo "=== Testing Worker Health Checking ==="
echo ""

# Step 1: Rebuild containers (to regenerate gRPC code)
echo "Step 1: Rebuilding containers..."
echo "  Building hdfs base image first..."
$COMPOSE build hdfs
echo "  Building nn and dn..."
$COMPOSE build nn dn

# Step 2: Start containers
echo ""
echo "Step 2: Starting containers..."
$COMPOSE up -d

# Step 3: Wait for services to be ready
echo ""
echo "Step 3: Waiting for services to initialize (10 seconds)..."
sleep 10

# Step 4: Check master logs for registration
echo ""
echo "Step 4: Checking master logs for worker registration..."
$COMPOSE logs nn | grep -i "registered\|worker pool" | tail -5 || true

# Step 5: Wait for heartbeats
echo ""
echo "Step 5: Waiting for heartbeats (15 seconds)..."
sleep 15

# Step 6: Check for heartbeat activity
echo ""
echo "Step 6: Checking for heartbeat activity in master logs..."
$COMPOSE logs nn | grep -i "heartbeat\|pong" | tail -10 || echo "No heartbeat logs found yet"

# Step 7: Check worker logs for heartbeat sending
echo ""
echo "Step 7: Checking worker logs for heartbeat activity..."
$COMPOSE logs dn | grep -i "heartbeat" | tail -5 || echo "No heartbeat logs in workers yet"

# Step 8: Test worker removal (stop a worker)
echo ""
echo "Step 8: Testing worker removal..."
echo "Stopping one worker container..."
$COMPOSE stop dn 2>/dev/null || echo "Could not stop worker (may not be running)"

echo ""
echo "Waiting 20 seconds to see if master detects dead worker..."
sleep 20

echo ""
echo "Step 9: Checking if master detected dead worker..."
$COMPOSE logs nn | grep -i "removing\|inactive" | tail -5 || echo "No removal logs found"

# Step 10: Show current worker pool
echo ""
echo "Step 10: Current master logs (last 20 lines)..."
$COMPOSE logs nn | tail -20

echo ""
echo "=== Test Complete ==="
echo ""
echo "To view live logs:"
echo "  $COMPOSE logs -f nn    # Master logs"
echo "  $COMPOSE logs -f dn      # Worker logs"
echo ""
echo "To clean up:"
echo "  $COMPOSE down"

