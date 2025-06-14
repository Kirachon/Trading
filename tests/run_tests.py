#!/usr/bin/env python3
"""
Test runner for the algorithmic trading system.
"""
import os
import sys
import subprocess
import argparse
from pathlib import Path

def run_unit_tests():
    """Run unit tests."""
    print("🧪 Running unit tests...")
    result = subprocess.run([
        sys.executable, '-m', 'pytest', 
        'tests/unit/', 
        '-v', 
        '--tb=short',
        '--cov=strategy_engine',
        '--cov=risk_management',
        '--cov=portfolio_accounting',
        '--cov-report=term-missing'
    ], cwd=Path(__file__).parent.parent)
    return result.returncode == 0

def run_integration_tests():
    """Run integration tests."""
    print("🔗 Running integration tests...")
    result = subprocess.run([
        sys.executable, '-m', 'pytest', 
        'tests/integration/', 
        '-v', 
        '--tb=short'
    ], cwd=Path(__file__).parent.parent)
    return result.returncode == 0

def run_performance_tests():
    """Run performance tests."""
    print("⚡ Running performance tests...")
    result = subprocess.run([
        sys.executable, '-m', 'pytest', 
        'tests/performance/', 
        '-v', 
        '--tb=short'
    ], cwd=Path(__file__).parent.parent)
    return result.returncode == 0

def run_all_tests():
    """Run all tests."""
    print("🚀 Running all tests...")
    
    success = True
    
    # Run unit tests
    if not run_unit_tests():
        print("❌ Unit tests failed")
        success = False
    else:
        print("✅ Unit tests passed")
    
    # Run integration tests
    if not run_integration_tests():
        print("❌ Integration tests failed")
        success = False
    else:
        print("✅ Integration tests passed")
    
    # Run performance tests
    if not run_performance_tests():
        print("❌ Performance tests failed")
        success = False
    else:
        print("✅ Performance tests passed")
    
    if success:
        print("\n🎉 All tests passed!")
    else:
        print("\n💥 Some tests failed!")
    
    return success

def main():
    """Main test runner."""
    parser = argparse.ArgumentParser(description='Run trading system tests')
    parser.add_argument('--unit', action='store_true', help='Run unit tests only')
    parser.add_argument('--integration', action='store_true', help='Run integration tests only')
    parser.add_argument('--performance', action='store_true', help='Run performance tests only')
    parser.add_argument('--coverage', action='store_true', help='Generate coverage report')
    
    args = parser.parse_args()
    
    # Set up environment
    os.environ['TESTING'] = 'true'
    
    success = True
    
    if args.unit:
        success = run_unit_tests()
    elif args.integration:
        success = run_integration_tests()
    elif args.performance:
        success = run_performance_tests()
    else:
        success = run_all_tests()
    
    if args.coverage:
        print("\n📊 Generating coverage report...")
        subprocess.run([
            sys.executable, '-m', 'coverage', 'html',
            '--directory=tests/coverage_html'
        ], cwd=Path(__file__).parent.parent)
        print("Coverage report generated in tests/coverage_html/")
    
    sys.exit(0 if success else 1)

if __name__ == '__main__':
    main()
