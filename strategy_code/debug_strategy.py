#!/usr/bin/env python3
"""
调试策略模块
实现固定的调度策略，强制调度到指定的层级
"""

from typing import Dict, Any

def debug_strategy_end(context: Dict[str, Any], config: Dict[str, Any] = None) -> Dict[str, Any]:
    """调试策略：强制调度到end节点"""
    possible_next = context.get("possible_next_stages", [])
    if possible_next:
        return {
            "next_stage": possible_next[0],
            "target_tier": "end"  # 强制调度到 end
        }
    return {
        "should_terminate": True
    }

def debug_strategy_edge(context: Dict[str, Any], config: Dict[str, Any] = None) -> Dict[str, Any]:
    """调试策略：强制调度到edge节点"""
    possible_next = context.get("possible_next_stages", [])
    if possible_next:
        return {
            "next_stage": possible_next[0],
            "target_tier": "edge"  # 强制调度到 edge
        }
    return {
        "should_terminate": True
    }

def debug_strategy_cloud(context: Dict[str, Any], config: Dict[str, Any] = None) -> Dict[str, Any]:
    """调试策略：强制调度到cloud节点"""
    possible_next = context.get("possible_next_stages", [])
    if possible_next:
        return {
            "next_stage": possible_next[0],
            "target_tier": "cloud"  # 强制调度到 cloud
        }
    return {
        "should_terminate": True
    }
