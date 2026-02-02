"""Alias module to avoid name collision with API RiskDataService.

This file re-exports the RiskDataCollectorService defined in
`risk_monitor.services.risk_data_service` under a distinct module name so
that the API side (`api.services.risk_data_service`) can never be confused
with the collector when Python caches modules in `sys.modules`.
"""

from __future__ import annotations

from importlib import import_module

# Import the original collector implementation
_original_mod = import_module("risk_monitor.services.risk_data_service")
RiskDataCollectorService = getattr(_original_mod, "RiskDataCollectorService")

__all__ = ["RiskDataCollectorService"] 
