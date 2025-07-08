#!/usr/bin/env python3
"""
Simple test runner for pyodibel tests
"""

import sys
import os

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyodibel.tests.test_acqsimp import test_get_wikidata_entity

def main():
    print("Running test_get_wikidata_entity...")
    try:
        test_get_wikidata_entity()
        print("✅ Test passed successfully!")
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return 1
    return 0

if __name__ == "__main__":
    sys.exit(main()) 