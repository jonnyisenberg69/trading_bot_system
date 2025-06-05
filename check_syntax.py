#!/usr/bin/env python
import ast

try:
    with open('exchanges/websocket/ws_manager.py', 'r') as f:
        content = f.read()
    
    ast.parse(content)
    print('✅ Syntax is valid')
except SyntaxError as e:
    print(f'❌ Syntax error: {e}')
    print(f'Line {e.lineno}: {e.text}')
    print(f'Problem at character position {e.offset}') 