#!/usr/bin/env python3
"""Test setup and verify all components work"""
import os
import sys

def test_imports():
    """Test all required imports"""
    print("Testing imports...")
    try:
        import fastapi
        print("✓ FastAPI installed")
    except ImportError:
        print("✗ FastAPI missing - run: pip install fastapi")
        return False
    
    try:
        import uvicorn
        print("✓ Uvicorn installed")
    except ImportError:
        print("✗ Uvicorn missing - run: pip install uvicorn[standard]")
        return False
    
    try:
        import pandas
        print("✓ Pandas installed")
    except ImportError:
        print("✗ Pandas missing - run: pip install pandas")
        return False
    
    try:
        from supabase import create_client
        print("✓ Supabase installed")
    except ImportError:
        print("✗ Supabase missing - run: pip install supabase")
        return False
    
    return True

def test_env():
    """Test environment variables"""
    print("\nTesting environment...")
    from dotenv import load_dotenv
    load_dotenv()
    
    required = ["SUPABASE_URL", "SUPABASE_SERVICE_ROLE_KEY", "ACTIONS_API_KEY"]
    missing = []
    
    for var in required:
        if os.getenv(var):
            print(f"✓ {var} set")
        else:
            print(f"✗ {var} missing")
            missing.append(var)
    
    return len(missing) == 0

def test_supabase():
    """Test Supabase connection"""
    print("\nTesting Supabase connection...")
    try:
        from supabase_client import supabase_client
        sb = supabase_client()
        
        # Try to query technicians table
        result = sb.table("technicians").select("*").limit(1).execute()
        print(f"✓ Connected to Supabase ({len(result.data)} test records)")
        return True
    except Exception as e:
        print(f"✗ Supabase connection failed: {e}")
        return False

def test_frontend():
    """Test frontend files exist"""
    print("\nTesting frontend files...")
    files = [
        "frontend/scheduler.html",
        "frontend/scheduler.js",
        "frontend/scheduler.css"
    ]
    
    all_exist = True
    for f in files:
        if os.path.exists(f):
            print(f"✓ {f} exists")
        else:
            print(f"✗ {f} missing")
            all_exist = False
    
    return all_exist

def main():
    print("=" * 60)
    print("UNIFIED SCHEDULER - SETUP TEST")
    print("=" * 60)
    
    results = {
        "imports": test_imports(),
        "environment": test_env(),
        "supabase": test_supabase(),
        "frontend": test_frontend()
    }
    
    print("\n" + "=" * 60)
    if all(results.values()):
        print("✅ ALL TESTS PASSED - Ready to run!")
        print("\nStart the server with:")
        print("  python scheduler_api_unified.py")
        print("\nThen open: http://localhost:8000")
        return 0
    else:
        print("❌ SOME TESTS FAILED")
        print("\nFailed:", [k for k, v in results.items() if not v])
        return 1

if __name__ == "__main__":
    sys.exit(main())
