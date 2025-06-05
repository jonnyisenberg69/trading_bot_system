# 🤖 Bot Monitoring Guide

## 🔍 **What Was Happening**

Your bot showed as "running" in the frontend, but **it wasn't actually executing any trading logic**. This was because the system was only **simulating** running bots instead of actually starting them.

### **The Problem** ❌
```python
# Old code in BotManager.start_instance():
# Note: For now, we'll simulate a running process
instance.process = None  # Placeholder - NOT ACTUALLY RUNNING!
instance.status = BotStatus.RUNNING  # Just marking as "running"
```

**Result**: Frontend shows "running" but no real trading happens.

---

## ✅ **What I Fixed**

### **1. Created Real Strategy Runner** (`bots/strategy_runner.py`)
- **Actual process execution** instead of simulation
- **Real strategy logic** from `bot_manager/strategies/passive_quoting.py`
- **Proper logging** and error handling
- **Signal handling** for graceful shutdown

### **2. Updated Bot Manager** (`api/services/bot_manager.py`)
- **Real subprocess spawning** with PID tracking
- **Process health monitoring** every 5 seconds
- **Automatic log file creation** in `logs/bots/{instance_id}/`
- **Proper error detection** when processes die

### **3. Enhanced API Endpoints** (`api/routes/bot_routes.py`)
- **`GET /api/bots/{id}/status`** - Detailed status with process info
- **`GET /api/bots/{id}/logs`** - Real-time log access
- **`GET /api/bots/{id}/performance`** - Performance statistics

### **4. Monitoring Tools**
- **`restart_bot.py`** - Restart existing bots with real strategy
- **`monitor_bot.py`** - Real-time monitoring dashboard

---

## 🚀 **How to Monitor Your Bots Now**

### **Step 1: Create a New Bot**
Since your previous bot was deleted, create a new one:
1. Go to **http://localhost:3000/bots**
2. Click **"New Bot"**
3. Enter **BERA/USDT** (or any trading pair)
4. Select your exchanges
5. Configure parameters
6. Click **"Create Bot"**

### **Step 2: Start the Bot**
Click the **▶️ Start** button in the frontend.

### **Step 3: Verify It's Really Running**

#### **Method 1: API Status Check**
```bash
# Get your bot ID from the frontend, then:
curl http://localhost:8000/api/bots/{INSTANCE_ID}/status | python -m json.tool
```

**Look for:**
```json
{
  "process_info": {
    "pid": 12345,
    "is_alive": true
  },
  "status": "running"
}
```

#### **Method 2: Real-Time Monitor**
```bash
python monitor_bot.py
```

**You'll see:**
```
🤖 Bot #1: passive_quoting_BERAUSDT_1749051517
   Symbol: BERA/USDT
   Exchanges: binance, bybit, hyperliquid, mexc, gateio, bitget
   Status: 🟢 RUNNING
   Process: PID 12345 ✅ Alive
   Last Heartbeat: 2s ago
   📋 Recent Logs:
      Starting strategy execution
      Creating passive_quoting strategy for BERA/USDT...
      Starting passive quoting strategy
```

#### **Method 3: Check Log Files**
```bash
# View live logs
tail -f logs/bots/{INSTANCE_ID}/{INSTANCE_ID}.log

# Or via API
curl http://localhost:8000/api/bots/{INSTANCE_ID}/logs
```

#### **Method 4: Process Check**
```bash
# Check if bot process is running
ps aux | grep {INSTANCE_ID}
```

---

## 🎯 **Signs Your Bot is ACTUALLY Running**

### ✅ **Good Signs:**
- **Process ID (PID)** is shown and alive
- **Log file exists** and has recent entries
- **Heartbeat** is recent (< 10 seconds ago)
- **Strategy logs** show "Starting passive quoting strategy"
- **Process check** shows running Python process

### ❌ **Bad Signs (Simulation):**
- **No PID** or PID shows as dead
- **No log file** or empty log file
- **No heartbeat** or very old heartbeat
- **No process** found when checking `ps aux`

---

## 📊 **What to Expect When Bot is REALLY Running**

### **Passive Quoting Strategy Activity:**
```
2025-06-04 15:38:41 - Starting strategy execution
2025-06-04 15:38:41 - Creating passive_quoting strategy for BERA/USDT on 6 exchanges
2025-06-04 15:38:41 - Starting passive quoting strategy
2025-06-04 15:38:42 - Validated config with 1 quote lines
2025-06-04 15:38:42 - Line 0: Placed bid orders @ 45123.4567 qty=19.83456789 on 6 exchanges
2025-06-04 15:38:42 - Line 0: Placed ask orders @ 45147.8901 qty=20.15643211 on 6 exchanges
```

### **Expected Bot Behavior:**
- **Places orders** on configured exchanges
- **Manages timeouts** (cancels orders after timeout)
- **Handles drift** (cancels if price moves too much)
- **Randomizes quantities** (within configured range)
- **Updates every second** (strategy loop)

---

## 🛠️ **Troubleshooting**

### **Bot Shows Running But No Activity:**
```bash
# Check detailed status
curl http://localhost:8000/api/bots/{INSTANCE_ID}/status

# If process is dead but status is "running":
python restart_bot.py
```

### **Bot Won't Start:**
1. **Check API server** is running: `python api/main.py`
2. **Check exchange connections** in frontend
3. **Verify trading pair** is valid (e.g., BERA/USDT)
4. **Check logs** for startup errors

### **No Logs Generated:**
- Bot might not be actually starting
- Check if `logs/bots/` directory exists
- Verify bot has write permissions

---

## 📍 **Quick Reference**

### **Key Files:**
- **Bot Instance Data**: `data/bots/{instance_id}.json`
- **Bot Configuration**: `data/bots/{instance_id}_config.json`
- **Bot Logs**: `logs/bots/{instance_id}/{instance_id}.log`
- **Strategy Code**: `bot_manager/strategies/passive_quoting.py`

### **Key Endpoints:**
- **List Bots**: `GET /api/bots/`
- **Bot Status**: `GET /api/bots/{id}/status`
- **Bot Logs**: `GET /api/bots/{id}/logs`
- **Start Bot**: `POST /api/bots/{id}/start`
- **Stop Bot**: `POST /api/bots/{id}/stop`

### **Monitoring Commands:**
```bash
# Real-time monitor
python monitor_bot.py

# Restart bots with real strategy
python restart_bot.py

# Check API health
curl http://localhost:8000/api/system/health

# View live logs
tail -f logs/bots/{instance_id}/{instance_id}.log
```

---

## 🎉 **Summary**

**Before**: Bot status was just simulated - no actual trading
**After**: Real process execution with full monitoring capabilities

Your next bot creation will **actually run the trading strategy** and you'll have multiple ways to verify and monitor its activity! 🚀 