#!/bin/bash
echo "Step 01 - Starting - db upgrade"
superset db upgrade
echo "Step 01 - Complete - db upgraded"

echo "Step 02 - Starting - Setting up admin user"
superset fab create-admin --username admin --firstname Superset --lastname Admin --email admin@superset.com --password admin
echo "Step 02 - Complete - Setting up admin user"

echo "Step 03 - Starting - Setting up roles and perms"
superset init
echo "Step 03 - Complete - Setting up roles and perms"

echo "Step 04 - Starting - Load examples"
superset load_examples
echo "Step 04 - Complete - Load examples"
