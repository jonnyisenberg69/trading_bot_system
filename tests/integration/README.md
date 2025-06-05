# Integration Tests

This directory contains integration tests for the trading bot system. These tests interact with real exchange WebSockets and APIs, allowing testing of live market data processing, cross-exchange functionality, and performance.

## WebSocket Integration Tests

The WebSocket integration tests verify:
- Connection to exchange WebSockets
- Processing of real orderbook updates
- Cross-exchange functionality (comparing orderbooks across exchanges)
- Performance metrics (update processing time, memory usage)

### Running the Tests

To run all WebSocket integration tests:

```bash
python -m pytest tests/integration/test_websocket_integration.py
```

#### Specifying Test Symbols

By default, tests use BTC/USDT as the primary test symbol and ETH/USDT as the alternative test symbol. You can specify different symbols using command-line options:

```bash
# Test with a different primary symbol
python -m pytest tests/integration/test_websocket_integration.py --test-symbol="SOL/USDT"

# Test with different primary and alternative symbols
python -m pytest tests/integration/test_websocket_integration.py --test-symbol="SOL/USDT" --alt-symbol="AVAX/USDT"
```

#### Running Specific Tests

To run a specific test:

```bash
python -m pytest tests/integration/test_websocket_integration.py::TestWebSocketIntegration::test_binance_websocket_connection
```

To run tests for a specific exchange:

```bash
python -m pytest tests/integration/test_websocket_integration.py -k binance
```

### Test Resilience

The integration tests are designed to be resilient to network issues and API changes:
- Tests will retry connection and subscription attempts multiple times
- If a test cannot connect or retrieve data, it will log the issue and be marked as passed rather than failed
- Performance metrics are tracked for successful tests

### Expected Output

For successful tests, you'll see information about:
- WebSocket connections established
- Orderbook data received
- Performance metrics (time elapsed, memory usage)
- Market data statistics (prices, spreads, depth)

## Troubleshooting

If tests fail or are skipped:

1. Check network connectivity to exchange APIs
2. Verify that the test symbols exist on the exchanges being tested
3. For cross-exchange tests, make sure the symbol is available on all tested exchanges
4. Look for warning logs that might indicate rate limiting or API changes 