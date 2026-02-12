#!/usr/bin/env python3
"""
DEX Data Fetcher Runner
Runs fetcher for Hyperliquid
"""

import subprocess
import sys
import os
from datetime import datetime

def run_fetcher(name, script_path):
    """Run a fetcher script and report status"""
    print(f"\n{'='*60}")
    print(f"🚀 Running {name} fetcher...")
    print('='*60)
    
    try:
        result = subprocess.run(
            [sys.executable, script_path],
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            capture_output=False,
            timeout=300  # 5 minute timeout per fetcher
        )
        
        if result.returncode == 0:
            print(f"✅ {name} fetcher completed successfully")
            return True
        else:
            print(f"⚠️ {name} fetcher completed with errors")
            return False
    except subprocess.TimeoutExpired:
        print(f"❌ {name} fetcher timed out")
        return False
    except Exception as e:
        print(f"❌ {name} fetcher error: {e}")
        return False

def main():
    print("="*60)
    print("🐋 Multi-DEX Data Fetcher")
    print(f"⏰ Started at: {datetime.utcnow().isoformat()}Z")
    print("="*60)
    
    # Get the base directory
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    fetchers_dir = os.path.join(base_dir, 'fetchers')
    
    # Ensure data directory exists
    os.makedirs(os.path.join(base_dir, 'data'), exist_ok=True)
    
    results = {}
    
    # Run Hyperliquid fetcher
    results['Hyperliquid'] = run_fetcher(
        'Hyperliquid',
        os.path.join(fetchers_dir, 'fetch_hyperliquid.py')
    )
    
    # Summary
    print("\n" + "="*60)
    print("📊 Fetch Summary")
    print("="*60)
    
    for dex, success in results.items():
        status = "✅" if success else "❌"
        print(f"  {status} {dex}")
    
    successful = sum(results.values())
    total = len(results)
    print(f"\n  {successful}/{total} fetchers completed successfully")
    print(f"⏰ Completed at: {datetime.utcnow().isoformat()}Z")
    
    # Exit with error if any fetcher failed
    if successful < total:
        sys.exit(1)

if __name__ == "__main__":
    main()
