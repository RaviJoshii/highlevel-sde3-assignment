#!/bin/sh
set -e

echo "=================================================================="
echo "          OPPORTUNITIES BULK STAGE MOVE ENGINE STARTUP            "
echo "=================================================================="

echo "[Docker] Checking MongoDB readiness..."
until node -e "const mongoose = require('mongoose'); mongoose.connect(process.env.MONGODB_URI || 'mongodb://mongodb:27017/opportunities_crm', { serverSelectionTimeoutMS: 2000 }).then(() => { mongoose.disconnect(); process.exit(0); }).catch(() => process.exit(1));"; do
  echo "[Docker] Waiting for MongoDB..."
  sleep 1
done
echo "[Docker] Connected to MongoDB successfully!"

echo ""
echo "[Docker] Step 1: Running Automated Hard-Scenario Test Suite..."
npm test

echo ""
echo "[Docker] Step 2: Seeding initial dataset..."
npm run seed

echo ""
echo "[Docker] Step 3: Starting Opportunities API & Background Worker Engine..."
npm start
