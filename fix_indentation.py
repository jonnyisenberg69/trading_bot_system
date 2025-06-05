#!/usr/bin/env python
"""
Fix indentation issues in ws_manager.py
"""

def fix_ws_manager_indentation():
    with open('exchanges/websocket/ws_manager.py', 'r') as f:
        lines = f.readlines()
    
    # Fix specific lines with indentation issues
    fixes = [
        # Line 1217: Add proper indentation
        (1216, "                    result['id'] = str(message.get('t', ''))\n"),
        (1217, "                    result['price'] = float(message.get('p', 0))\n"),  
        (1218, "                    result['amount'] = float(message.get('q', 0))\n"),
        (1219, "                    result['side'] = 'sell' if message.get('m', False) else 'buy'\n"),
        (1220, "                    result['symbol'] = message.get('s', '')\n"),
        (1221, "                    result['timestamp'] = message.get('T', 0)\n"),
    ]
    
    # Apply fixes
    for line_num, fixed_line in fixes:
        if line_num < len(lines):
            lines[line_num] = fixed_line
    
    with open('exchanges/websocket/ws_manager.py', 'w') as f:
        f.writelines(lines)
    
    print("✅ Fixed indentation issues in ws_manager.py")

if __name__ == "__main__":
    fix_ws_manager_indentation() 