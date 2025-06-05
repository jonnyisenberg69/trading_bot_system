# Order Flow Integration Tests

This directory contains integration tests for order placement, cancellation, and tracking across all supported exchanges.

## Test Description

The tests are designed to:

1. Place a limit order for BTC/USDT at $10,000 (a price that will never fill)
2. Wait 10 seconds and verify the order is tracked correctly in the system
3. Cancel the order and verify cancellation succeeded

These tests validate the entire order flow process from placement to cancellation, ensuring that the order tracking system is working correctly.

## Setup

### Exchange Credentials

Before running the tests, you need to set up exchange credentials:

1. Copy the sample credentials file:
   ```
   cp tests/fixtures/exchange_credentials.sample.json tests/fixtures/exchange_credentials.json
   ```

2. Edit the `exchange_credentials.json` file with your API keys:
   ```json
   {
       "binance_spot": {
           "api_key": "YOUR_BINANCE_API_KEY",
           "secret": "YOUR_BINANCE_SECRET",
           "sandbox": true,
           "market_type": "spot"
       },
       ...
   }
   ```

3. Make sure to set `"sandbox": true` to use testnet for each exchange. Never run these tests on production accounts with real funds!

### Environment Variables

Alternatively, you can set credentials via environment variables:

```bash
export BINANCE_API_KEY="your_api_key"
export BINANCE_SECRET="your_secret"
```

## Running the Tests

### Using pytest

Run all order flow tests:

```bash
pytest tests/integration/test_order_flow.py -v
```

Run for a specific exchange:

```bash
pytest tests/integration/test_order_flow.py::test_individual_exchange_order_flow -v
```

### Using the Helper Script

For quick testing during development, you can use the helper script:

```bash
# Test all exchanges
python tests/run_order_flow_tests.py

# Test a specific exchange
python tests/run_order_flow_tests.py binance_spot
```

## Test Parameters

You can modify these parameters in `test_order_flow.py`:

- `TEST_SYMBOL`: The trading pair to test with (default: "BTC/USDT")
- `TEST_PRICE`: The order price (default: 10000.00)
- `TEST_AMOUNT`: The order amount (default: 0.001)
- `WAIT_TIME`: Seconds to wait for order tracking (default: 10)

## Troubleshooting

### Invalid API Keys

If you see errors like "Invalid API-key" or "Signature for this request is not valid", check that:
- Your API keys are entered correctly
- The API keys have permission for trading
- You're using testnet API keys if `sandbox` is set to `true`

### Network Issues

If the tests fail due to network issues:
- Check your internet connection
- Verify that the exchange's API servers are operational
- Try increasing timeouts in the test code if needed

### Rate Limiting

If you hit rate limits:
- Increase the wait time between tests
- Run tests for one exchange at a time
- Ensure you're not running other scripts that use the same API keys 