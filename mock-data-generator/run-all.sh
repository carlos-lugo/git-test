#!/bin/bash

# This script runs all the mock data generator scripts in the correct order.

echo "Running 1-students-seed.py..."
python 1-students-seed.py

echo "Running 2-venues-seed.py..."
python 2-venues-seed.py

echo "Running 3-bank-seed.py..."
python 3-bank-seed.py

echo "Running 4-exam-seed.py..."
python 4-exam-seed.py

echo "Running 5-examhold-seed.py..."
python 5-examhold-seed.py

echo "Running 6-application-seed.py..."
python 6-application-seed.py

echo "Running 7-payment-seed.py..."
python 7-payment-seed.py

echo "Running 8-certification-seed.py..."
python 8-certification-seed.py

echo "All mock data generation scripts have been executed."
