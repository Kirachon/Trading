#!/usr/bin/env python3
"""
Performance Benchmarking for Strategy Engine Optimizations

This script benchmarks the performance improvements from Phase 2 optimizations,
specifically comparing the old DataFrame-based approach with the new rolling
window approach.
"""

import asyncio
import time
import logging
import sys
import os
from datetime import datetime, timedelta
from typing import Dict, List, Any
import pandas as pd
import numpy as np

# Add shared modules to path
sys.path.append('/app')
from shared.rolling_window import RollingWindow, OHLCVPoint, IncrementalIndicators

logger = logging.getLogger(__name__)


class PerformanceBenchmark:
    """Benchmark performance improvements in strategy engine."""
    
    def __init__(self):
        self.results = {}
        
    def generate_test_data(self, num_points: int = 1000) -> List[Dict[str, Any]]:
        """Generate test OHLCV data for benchmarking."""
        base_price = 100.0
        data = []
        
        for i in range(num_points):
            # Simulate realistic price movement
            change = np.random.normal(0, 0.01)  # 1% volatility
            base_price *= (1 + change)
            
            high = base_price * (1 + abs(np.random.normal(0, 0.005)))
            low = base_price * (1 - abs(np.random.normal(0, 0.005)))
            volume = np.random.uniform(1000, 10000)
            
            data.append({
                'timestamp': int((datetime.now() + timedelta(minutes=i)).timestamp() * 1000),
                'datetime': datetime.now() + timedelta(minutes=i),
                'open': base_price,
                'high': high,
                'low': low,
                'close': base_price,
                'volume': volume
            })
            
        return data
    
    def benchmark_dataframe_approach(self, data: List[Dict[str, Any]], iterations: int = 100) -> Dict[str, float]:
        """Benchmark the old DataFrame-based approach."""
        logger.info("Benchmarking DataFrame approach...")
        
        # Setup
        buffer = []
        buffer_size = 500
        
        # Timing variables
        total_append_time = 0
        total_indicator_time = 0
        total_memory_usage = 0
        
        start_time = time.time()
        
        for iteration in range(iterations):
            for data_point in data:
                # Simulate old approach: append to list and recreate DataFrame
                append_start = time.time()
                
                buffer.append(data_point)
                if len(buffer) > buffer_size:
                    buffer = buffer[-buffer_size:]
                
                # Create DataFrame (expensive operation)
                df = pd.DataFrame(buffer)
                df.set_index('datetime', inplace=True)
                
                append_time = time.time() - append_start
                total_append_time += append_time
                
                # Calculate indicators (expensive with full recalculation)
                if len(df) >= 50:
                    indicator_start = time.time()
                    
                    # Simulate indicator calculations
                    sma_10 = df['close'].rolling(10).mean()
                    sma_20 = df['close'].rolling(20).mean()
                    sma_50 = df['close'].rolling(50).mean()
                    ema_12 = df['close'].ewm(span=12).mean()
                    ema_26 = df['close'].ewm(span=26).mean()
                    
                    indicator_time = time.time() - indicator_start
                    total_indicator_time += indicator_time
                
                # Estimate memory usage
                total_memory_usage += sys.getsizeof(df)
        
        total_time = time.time() - start_time
        
        return {
            'total_time': total_time,
            'avg_append_time': total_append_time / (iterations * len(data)),
            'avg_indicator_time': total_indicator_time / (iterations * len(data)),
            'avg_memory_usage': total_memory_usage / (iterations * len(data)),
            'operations_per_second': (iterations * len(data)) / total_time
        }
    
    def benchmark_rolling_window_approach(self, data: List[Dict[str, Any]], iterations: int = 100) -> Dict[str, float]:
        """Benchmark the new rolling window approach."""
        logger.info("Benchmarking Rolling Window approach...")
        
        # Setup
        rolling_window = RollingWindow(max_size=500)
        indicators = IncrementalIndicators()
        
        # Timing variables
        total_append_time = 0
        total_indicator_time = 0
        total_memory_usage = 0
        
        start_time = time.time()
        
        for iteration in range(iterations):
            for data_point in data:
                # Simulate new approach: efficient rolling window
                append_start = time.time()
                
                ohlcv_point = OHLCVPoint(
                    timestamp=data_point['timestamp'],
                    datetime_obj=data_point['datetime'],
                    open_price=data_point['open'],
                    high=data_point['high'],
                    low=data_point['low'],
                    close=data_point['close'],
                    volume=data_point['volume']
                )
                
                rolling_window.append(ohlcv_point)
                
                append_time = time.time() - append_start
                total_append_time += append_time
                
                # Calculate indicators (efficient incremental updates)
                if rolling_window.is_ready(50):
                    indicator_start = time.time()
                    
                    # Simulate efficient indicator calculations
                    indicators.update_sma(rolling_window, 10, 'sma_10')
                    indicators.update_sma(rolling_window, 20, 'sma_20')
                    indicators.update_sma(rolling_window, 50, 'sma_50')
                    indicators.update_ema(rolling_window, 12, 'ema_12')
                    indicators.update_ema(rolling_window, 26, 'ema_26')
                    
                    indicator_time = time.time() - indicator_start
                    total_indicator_time += indicator_time
                
                # Estimate memory usage (much lower)
                total_memory_usage += sys.getsizeof(rolling_window.data)
        
        total_time = time.time() - start_time
        
        return {
            'total_time': total_time,
            'avg_append_time': total_append_time / (iterations * len(data)),
            'avg_indicator_time': total_indicator_time / (iterations * len(data)),
            'avg_memory_usage': total_memory_usage / (iterations * len(data)),
            'operations_per_second': (iterations * len(data)) / total_time
        }
    
    def run_benchmark(self, num_data_points: int = 1000, iterations: int = 100):
        """Run complete benchmark comparing both approaches."""
        logger.info(f"Starting performance benchmark with {num_data_points} data points, {iterations} iterations")
        
        # Generate test data
        test_data = self.generate_test_data(num_data_points)
        
        # Benchmark DataFrame approach
        df_results = self.benchmark_dataframe_approach(test_data, iterations)
        
        # Benchmark Rolling Window approach
        rw_results = self.benchmark_rolling_window_approach(test_data, iterations)
        
        # Calculate improvements
        improvements = {
            'total_time_improvement': ((df_results['total_time'] - rw_results['total_time']) / df_results['total_time']) * 100,
            'append_time_improvement': ((df_results['avg_append_time'] - rw_results['avg_append_time']) / df_results['avg_append_time']) * 100,
            'indicator_time_improvement': ((df_results['avg_indicator_time'] - rw_results['avg_indicator_time']) / df_results['avg_indicator_time']) * 100,
            'memory_improvement': ((df_results['avg_memory_usage'] - rw_results['avg_memory_usage']) / df_results['avg_memory_usage']) * 100,
            'throughput_improvement': ((rw_results['operations_per_second'] - df_results['operations_per_second']) / df_results['operations_per_second']) * 100
        }
        
        # Store results
        self.results = {
            'dataframe_approach': df_results,
            'rolling_window_approach': rw_results,
            'improvements': improvements,
            'test_parameters': {
                'num_data_points': num_data_points,
                'iterations': iterations,
                'total_operations': num_data_points * iterations
            }
        }
        
        return self.results
    
    def print_results(self):
        """Print benchmark results in a formatted way."""
        if not self.results:
            logger.error("No benchmark results available. Run benchmark first.")
            return
        
        print("\n" + "="*80)
        print("STRATEGY ENGINE PERFORMANCE BENCHMARK RESULTS")
        print("="*80)
        
        params = self.results['test_parameters']
        print(f"Test Parameters:")
        print(f"  Data Points: {params['num_data_points']:,}")
        print(f"  Iterations: {params['iterations']:,}")
        print(f"  Total Operations: {params['total_operations']:,}")
        print()
        
        df_results = self.results['dataframe_approach']
        rw_results = self.results['rolling_window_approach']
        improvements = self.results['improvements']
        
        print("Performance Comparison:")
        print(f"{'Metric':<30} {'DataFrame':<15} {'RollingWindow':<15} {'Improvement':<15}")
        print("-" * 75)
        print(f"{'Total Time (s)':<30} {df_results['total_time']:<15.3f} {rw_results['total_time']:<15.3f} {improvements['total_time_improvement']:<15.1f}%")
        print(f"{'Avg Append Time (ms)':<30} {df_results['avg_append_time']*1000:<15.3f} {rw_results['avg_append_time']*1000:<15.3f} {improvements['append_time_improvement']:<15.1f}%")
        print(f"{'Avg Indicator Time (ms)':<30} {df_results['avg_indicator_time']*1000:<15.3f} {rw_results['avg_indicator_time']*1000:<15.3f} {improvements['indicator_time_improvement']:<15.1f}%")
        print(f"{'Avg Memory Usage (bytes)':<30} {df_results['avg_memory_usage']:<15.0f} {rw_results['avg_memory_usage']:<15.0f} {improvements['memory_improvement']:<15.1f}%")
        print(f"{'Operations/Second':<30} {df_results['operations_per_second']:<15.0f} {rw_results['operations_per_second']:<15.0f} {improvements['throughput_improvement']:<15.1f}%")
        print()
        
        print("Summary:")
        print(f"  • Total performance improvement: {improvements['total_time_improvement']:.1f}%")
        print(f"  • Memory usage reduction: {improvements['memory_improvement']:.1f}%")
        print(f"  • Throughput increase: {improvements['throughput_improvement']:.1f}%")
        print(f"  • Data append efficiency: {improvements['append_time_improvement']:.1f}%")
        print(f"  • Indicator calculation efficiency: {improvements['indicator_time_improvement']:.1f}%")
        print("\n" + "="*80)


def main():
    """Main benchmark execution."""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    benchmark = PerformanceBenchmark()
    
    # Run benchmark with different configurations
    configurations = [
        {'num_data_points': 500, 'iterations': 50},
        {'num_data_points': 1000, 'iterations': 100},
        {'num_data_points': 2000, 'iterations': 50}
    ]
    
    for i, config in enumerate(configurations, 1):
        print(f"\n{'='*20} BENCHMARK {i}/{len(configurations)} {'='*20}")
        results = benchmark.run_benchmark(**config)
        benchmark.print_results()


if __name__ == "__main__":
    main()
