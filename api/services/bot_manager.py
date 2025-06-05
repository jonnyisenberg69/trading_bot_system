"""
Bot Manager service for managing trading bot instances.

Handles starting, stopping, and monitoring bot instances with different strategies.
"""

import asyncio
import json
import os
import signal
import subprocess
import time
from typing import Dict, Any, List, Optional, Set
from datetime import datetime
from pathlib import Path
import structlog
from enum import Enum

logger = structlog.get_logger(__name__)


class BotStatus(str, Enum):
    """Bot instance status."""
    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    ERROR = "error"


class BotInstance:
    """Represents a running bot instance."""
    
    def __init__(
        self,
        instance_id: str,
        strategy: str,
        symbol: str,
        exchanges: List[str],
        config: Dict[str, Any]
    ):
        self.instance_id = instance_id
        self.strategy = strategy
        self.symbol = symbol
        self.exchanges = exchanges
        self.config = config
        self.status = BotStatus.STOPPED
        self.process: Optional[subprocess.Popen] = None
        self.started_at: Optional[datetime] = None
        self.last_heartbeat: Optional[datetime] = None
        self.error_message: Optional[str] = None
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "instance_id": self.instance_id,
            "strategy": self.strategy,
            "symbol": self.symbol,
            "exchanges": self.exchanges,
            "config": self.config,
            "status": self.status.value,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "last_heartbeat": self.last_heartbeat.isoformat() if self.last_heartbeat else None,
            "error_message": self.error_message,
            "uptime_seconds": (
                (datetime.now() - self.started_at).total_seconds() 
                if self.started_at and self.status == BotStatus.RUNNING 
                else None
            )
        }


class BotManager:
    """
    Manages multiple bot instances.
    
    Each bot instance runs as a separate process with its own strategy,
    symbol, and exchange configuration.
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.instances: Dict[str, BotInstance] = {}
        self.running = False
        self.monitor_task: Optional[asyncio.Task] = None
        self.logger = logger.bind(component="BotManager")
        
        # Create bot data directory
        self.data_dir = Path("data/bots")
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
    async def start(self) -> None:
        """Start the bot manager."""
        if self.running:
            return
            
        self.running = True
        self.logger.info("Starting bot manager")
        
        # Load existing bot instances from disk
        await self._load_instances()
        
        # Start monitoring task
        self.monitor_task = asyncio.create_task(self._monitor_instances())
        
        self.logger.info("Bot manager started")
        
    async def stop(self) -> None:
        """Stop the bot manager and all instances."""
        if not self.running:
            return
            
        self.running = False
        self.logger.info("Stopping bot manager")
        
        # Stop monitoring task
        if self.monitor_task:
            self.monitor_task.cancel()
            try:
                await self.monitor_task
            except asyncio.CancelledError:
                pass
                
        # Stop all bot instances
        for instance in self.instances.values():
            await self._stop_instance(instance)
            
        self.logger.info("Bot manager stopped")
        
    async def create_instance(
        self,
        strategy: str,
        symbol: str,
        exchanges: List[str],
        config: Dict[str, Any] = None
    ) -> str:
        """
        Create a new bot instance.
        
        Args:
            strategy: Trading strategy name
            symbol: Trading symbol (e.g., "BTC/USDT")
            exchanges: List of exchanges to trade on
            config: Additional configuration
            
        Returns:
            Instance ID
        """
        # Generate unique instance ID
        timestamp = int(time.time())
        instance_id = f"{strategy}_{symbol.replace('/', '')}_{timestamp}"
        
        # Create instance
        instance = BotInstance(
            instance_id=instance_id,
            strategy=strategy,
            symbol=symbol,
            exchanges=exchanges,
            config=config or {}
        )
        
        self.instances[instance_id] = instance
        
        # Save instance to disk
        await self._save_instance(instance)
        
        self.logger.info(f"Created bot instance: {instance_id}")
        return instance_id
        
    async def start_instance(self, instance_id: str) -> bool:
        """
        Start a bot instance.
        
        Args:
            instance_id: Instance ID to start
            
        Returns:
            True if started successfully
        """
        if instance_id not in self.instances:
            self.logger.error(f"Instance not found: {instance_id}")
            return False
            
        instance = self.instances[instance_id]
        
        if instance.status in [BotStatus.RUNNING, BotStatus.STARTING]:
            self.logger.warning(f"Instance already running or starting: {instance_id}")
            return True
            
        self.logger.info(f"Starting bot instance: {instance_id}")
        
        try:
            instance.status = BotStatus.STARTING
            instance.error_message = None
            
            # Create instance config file
            config_file = self.data_dir / f"{instance_id}_config.json"
            instance_config = {
                "strategy": instance.strategy,
                "symbol": instance.symbol,
                "exchanges": instance.exchanges,
                "config": instance.config
            }
            
            with open(config_file, 'w') as f:
                json.dump(instance_config, f, indent=2)
            
            # Create log directory for this instance
            log_dir = Path("logs") / "bots" / instance_id
            log_dir.mkdir(parents=True, exist_ok=True)
            
            # Start actual bot process using the strategy runner
            cmd = [
                "python", "-m", "bots.strategy_runner",
                "--config", str(config_file),
                "--instance-id", instance_id,
                "--log-level", "INFO"
            ]
            
            # Start the subprocess with proper logging
            log_file = log_dir / f"{instance_id}.log"
            
            instance.process = subprocess.Popen(
                cmd,
                stdout=open(log_file, 'a'),
                stderr=subprocess.STDOUT,
                cwd=Path.cwd(),
                env=os.environ.copy()
            )
            
            # Give the process a moment to start
            await asyncio.sleep(1)
            
            # Check if process started successfully
            if instance.process.poll() is None:
                # Process is running
                instance.status = BotStatus.RUNNING
                instance.started_at = datetime.now()
                instance.last_heartbeat = datetime.now()
                
                self.logger.info(f"Bot instance started successfully: {instance_id} (PID: {instance.process.pid})")
            else:
                # Process failed to start
                instance.status = BotStatus.ERROR
                instance.error_message = f"Process exited immediately with code {instance.process.returncode}"
                instance.process = None
                
                self.logger.error(f"Bot instance failed to start: {instance_id}")
                return False
            
            await self._save_instance(instance)
            return True
            
        except Exception as e:
            instance.status = BotStatus.ERROR
            instance.error_message = str(e)
            instance.process = None
            await self._save_instance(instance)
            
            self.logger.error(f"Failed to start instance {instance_id}: {e}")
            return False
            
    async def stop_instance(self, instance_id: str) -> bool:
        """
        Stop a bot instance.
        
        Args:
            instance_id: Instance ID to stop
            
        Returns:
            True if stopped successfully
        """
        if instance_id not in self.instances:
            self.logger.error(f"Instance not found: {instance_id}")
            return False
            
        instance = self.instances[instance_id]
        return await self._stop_instance(instance)
        
    async def delete_instance(self, instance_id: str) -> bool:
        """
        Delete a bot instance.
        
        Args:
            instance_id: Instance ID to delete
            
        Returns:
            True if deleted successfully
        """
        if instance_id not in self.instances:
            self.logger.error(f"Instance not found: {instance_id}")
            return False
            
        instance = self.instances[instance_id]
        
        # Stop instance if running
        if instance.status in [BotStatus.RUNNING, BotStatus.STARTING]:
            await self._stop_instance(instance)
            
        # Remove from instances
        del self.instances[instance_id]
        
        # Remove instance file
        instance_file = self.data_dir / f"{instance_id}.json"
        if instance_file.exists():
            instance_file.unlink()
            
        # Remove config file
        config_file = self.data_dir / f"{instance_id}_config.json"
        if config_file.exists():
            config_file.unlink()
            
        self.logger.info(f"Deleted bot instance: {instance_id}")
        return True
        
    def get_instance(self, instance_id: str) -> Optional[BotInstance]:
        """Get bot instance by ID."""
        return self.instances.get(instance_id)
        
    def get_all_instances(self) -> List[BotInstance]:
        """Get all bot instances."""
        return list(self.instances.values())
        
    def get_running_instances(self) -> List[BotInstance]:
        """Get all running bot instances."""
        return [
            instance for instance in self.instances.values()
            if instance.status == BotStatus.RUNNING
        ]
        
    async def _stop_instance(self, instance: BotInstance) -> bool:
        """Stop a specific bot instance."""
        if instance.status in [BotStatus.STOPPED, BotStatus.STOPPING]:
            return True
            
        self.logger.info(f"Stopping bot instance: {instance.instance_id}")
        
        try:
            instance.status = BotStatus.STOPPING
            
            # Stop process if running
            if instance.process:
                try:
                    instance.process.terminate()
                    
                    # Wait for graceful shutdown
                    try:
                        instance.process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        # Force kill if not stopped gracefully
                        instance.process.kill()
                        instance.process.wait()
                        
                except Exception as e:
                    self.logger.error(f"Error stopping process for {instance.instance_id}: {e}")
                    
            instance.status = BotStatus.STOPPED
            instance.process = None
            
            await self._save_instance(instance)
            
            self.logger.info(f"Bot instance stopped: {instance.instance_id}")
            return True
            
        except Exception as e:
            instance.status = BotStatus.ERROR
            instance.error_message = str(e)
            await self._save_instance(instance)
            
            self.logger.error(f"Failed to stop instance {instance.instance_id}: {e}")
            return False
            
    async def _monitor_instances(self) -> None:
        """Monitor running instances for health and status."""
        while self.running:
            try:
                for instance in self.instances.values():
                    if instance.status == BotStatus.RUNNING:
                        # Check if process is still alive
                        if instance.process:
                            if instance.process.poll() is not None:
                                # Process has exited
                                instance.status = BotStatus.ERROR
                                instance.error_message = f"Process exited with code {instance.process.returncode}"
                                instance.process = None
                                self.logger.error(f"Bot process died: {instance.instance_id}")
                                await self._save_instance(instance)
                            else:
                                # Process is still running, update heartbeat
                                instance.last_heartbeat = datetime.now()
                                await self._save_instance(instance)
                        else:
                            # No process associated but marked as running - this shouldn't happen
                            self.logger.warning(f"Instance {instance.instance_id} marked as running but no process found")
                            instance.status = BotStatus.ERROR
                            instance.error_message = "No process found for running instance"
                            await self._save_instance(instance)
                        
                await asyncio.sleep(5)  # Check every 5 seconds
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in instance monitoring: {e}")
                await asyncio.sleep(5)
                
    async def _load_instances(self) -> None:
        """Load bot instances from disk."""
        try:
            for file_path in self.data_dir.glob("*.json"):
                if file_path.name.endswith("_config.json"):
                    continue  # Skip config files
                    
                try:
                    with open(file_path, 'r') as f:
                        data = json.load(f)
                        
                    instance = BotInstance(
                        instance_id=data["instance_id"],
                        strategy=data["strategy"],
                        symbol=data["symbol"],
                        exchanges=data["exchanges"],
                        config=data.get("config", {})
                    )
                    
                    # Try to load additional config from the separate config file
                    config_file = self.data_dir / f"{instance.instance_id}_config.json"
                    if config_file.exists():
                        try:
                            with open(config_file, 'r') as f:
                                config_data = json.load(f)
                                # Update the instance config with the more complete config
                                if "config" in config_data:
                                    instance.config = config_data["config"]
                        except Exception as e:
                            self.logger.warning(f"Failed to load config file for {instance.instance_id}: {e}")
                    
                    instance.status = BotStatus(data.get("status", BotStatus.STOPPED.value))
                    if data.get("started_at"):
                        instance.started_at = datetime.fromisoformat(data["started_at"])
                    if data.get("last_heartbeat"):
                        instance.last_heartbeat = datetime.fromisoformat(data["last_heartbeat"])
                    instance.error_message = data.get("error_message")
                    
                    # If instance was running, mark as stopped (since we're restarting)
                    if instance.status in [BotStatus.RUNNING, BotStatus.STARTING]:
                        instance.status = BotStatus.STOPPED
                        
                    self.instances[instance.instance_id] = instance
                    
                except Exception as e:
                    self.logger.error(f"Error loading instance from {file_path}: {e}")
                    
            self.logger.info(f"Loaded {len(self.instances)} bot instances")
            
        except Exception as e:
            self.logger.error(f"Error loading instances: {e}")
            
    async def _save_instance(self, instance: BotInstance) -> None:
        """Save bot instance to disk."""
        try:
            instance_file = self.data_dir / f"{instance.instance_id}.json"
            data = instance.to_dict()
            
            with open(instance_file, 'w') as f:
                json.dump(data, f, indent=2)
                
        except Exception as e:
            self.logger.error(f"Error saving instance {instance.instance_id}: {e}") 