#!/usr/bin/env python3
"""
Generate Solana wallet file from base58 private key and public key in exchange_keys.py
"""
import sys
import os
import json
import base58

# Add parent directory to sys.path for import
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.exchange_keys import EXCHANGE_CONFIGS

# Get Meteora DLMM config
config = EXCHANGE_CONFIGS['meteora_dlmm']
public_key = config['wallet_address']
private_key_b58 = config['private_key']

# Decode base58 private key
private_key_bytes = base58.b58decode(private_key_b58)

# Solana expects a list of 64 bytes (private + public key)
if len(private_key_bytes) == 64:
    keypair = list(private_key_bytes)
else:
    raise ValueError(f"Decoded private key is {len(private_key_bytes)} bytes, expected 64 bytes.")

wallet_json = keypair

# Write to file
wallet_path = os.path.join(os.path.dirname(__file__), 'solana_wallet.json')
with open(wallet_path, 'w') as f:
    json.dump(wallet_json, f)

print(f"Solana wallet file written to {wallet_path}") 