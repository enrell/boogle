"""
Provider Registry

Manages registration and discovery of book providers.
"""

import importlib
import pkgutil
from typing import Dict, Type, List, Optional, Any
from pathlib import Path

from src.providers.base import BaseBookProvider


class ProviderRegistry:
    """
    Central registry for all book providers.

    Providers are registered either:
    1. Via the @register_provider decorator
    2. Via auto-discovery of modules in the providers package

    Example:
        # Register via decorator
        @register_provider
        class MyProvider(BaseBookProvider):
            ...

        # Get provider instance
        provider = ProviderRegistry.get('myprovider')

        # List all providers
        for name in ProviderRegistry.list_sources():
            print(name)
    """

    _providers: Dict[str, BaseBookProvider] = {}
    _provider_classes: Dict[str, Type[BaseBookProvider]] = {}
    _config: Optional[Dict[str, Any]] = None

    @classmethod
    def register(cls, provider_class: Type[BaseBookProvider]) -> Type[BaseBookProvider]:
        """
        Register a provider class.

        This is typically used as a decorator:
            @register_provider
            class MyProvider(BaseBookProvider):
                ...

        Args:
            provider_class: The provider class to register

        Returns:
            The provider class (for decorator use)
        """
        # Create temporary instance to get source_name
        try:
            temp_instance = provider_class()
            source_name = temp_instance.source_name
            cls._provider_classes[source_name] = provider_class
        except Exception as e:
            raise ValueError(
                f"Failed to register provider {provider_class.__name__}: {e}"
            )

        return provider_class

    @classmethod
    def get(cls, source: str) -> BaseBookProvider:
        """
        Get a provider instance by name.

        Args:
            source: The provider source name

        Returns:
            BaseBookProvider: The provider instance

        Raises:
            ValueError: If provider not found
        """
        if source not in cls._providers:
            if source not in cls._provider_classes:
                # Try auto-discover in case provider module hasn't been imported
                cls.auto_discover()

                if source not in cls._provider_classes:
                    available = ", ".join(cls.list_sources())
                    raise ValueError(
                        f"Unknown provider: '{source}'. "
                        f"Available providers: {available}"
                    )

            # Instantiate and cache
            cls._providers[source] = cls._provider_classes[source]()

        return cls._providers[source]

    @classmethod
    def get_all(cls, config: Optional[Dict[str, Any]] = None) -> List[BaseBookProvider]:
        """
        Get all registered provider instances.

        Args:
            config: Optional configuration dict. If None, returns all registered providers.

        Returns:
            List[BaseBookProvider]: List of provider instances
        """
        cls._ensure_discovered()

        providers = []
        for source_name in cls._provider_classes:
            # Check config if provided
            if config and source_name in config:
                if not config[source_name].get("enabled", True):
                    continue

            providers.append(cls.get(source_name))

        # Sort by source_name for consistent ordering
        providers.sort(key=lambda p: p.source_name)
        return providers

    @classmethod
    def get_enabled(
        cls, config: Optional[Dict[str, Any]] = None
    ) -> List[BaseBookProvider]:
        """
        Get all enabled providers based on configuration.

        Args:
            config: Provider configuration. If None, uses get_default_config().

        Returns:
            List[BaseBookProvider]: List of enabled provider instances
        """
        if config is None:
            config = cls.get_default_config()

        cls._ensure_discovered()

        enabled = []
        for source_name, provider_config in config.items():
            if provider_config.get("enabled", True):
                try:
                    provider = cls.get(source_name)
                    enabled.append(provider)
                except ValueError as e:
                    print(f"Warning: {e}")

        # Sort by priority if available
        def sort_key(p):
            cfg = config.get(p.source_name, {})
            return cfg.get("priority", 999)

        enabled.sort(key=sort_key)
        return enabled

    @classmethod
    def list_sources(cls) -> List[str]:
        """
        List all available provider source names.

        Returns:
            List[str]: List of provider names
        """
        cls._ensure_discovered()
        return sorted(cls._provider_classes.keys())

    @classmethod
    def auto_discover(cls, package_path: str = "src.providers") -> None:
        """
        Auto-discover providers in the providers package.

        Scans the providers directory for modules and imports them,
        which triggers the @register_provider decorator.

        Args:
            package_path: Python import path to scan
        """
        try:
            import src.providers as providers_pkg

            for importer, modname, ispkg in pkgutil.iter_modules(
                providers_pkg.__path__, providers_pkg.__name__ + "."
            ):
                # Skip internal modules
                if modname.endswith((".base", ".registry", ".config")):
                    continue

                try:
                    importlib.import_module(modname)
                except Exception as e:
                    print(f"Warning: Failed to load provider module {modname}: {e}")

        except Exception as e:
            print(f"Warning: Auto-discovery failed: {e}")

    @classmethod
    def _ensure_discovered(cls) -> None:
        """Ensure auto-discovery has run at least once."""
        if not cls._provider_classes:
            cls.auto_discover()

    @classmethod
    def get_default_config(cls) -> Dict[str, Any]:
        """
        Get default provider configuration.

        Returns configuration for all registered providers,
        respecting their enabled_by_default setting.

        Returns:
            Dict[str, Any]: Provider configuration dictionary
        """
        cls._ensure_discovered()

        config = {}
        for source_name, provider_class in cls._provider_classes.items():
            try:
                temp_instance = provider_class()
                config[source_name] = {
                    "enabled": temp_instance.enabled_by_default,
                    "priority": 999,
                    "config": {},
                }
            except Exception:
                config[source_name] = {"enabled": False, "priority": 999, "config": {}}

        return config

    @classmethod
    def load_config_from_env(cls) -> Dict[str, Any]:
        """
        Load provider configuration from environment variables.

        Environment variable format:
            BOOGLE_PROVIDER_{SOURCE}_ENABLED=0|1
            BOOGLE_PROVIDER_{SOURCE}_PRIORITY=N

        Returns:
            Dict[str, Any]: Merged configuration
        """
        import os

        config = cls.get_default_config()

        for key, value in os.environ.items():
            if not key.startswith("BOOGLE_PROVIDER_"):
                continue

            # Parse: BOOGLE_PROVIDER_{SOURCE}_{SETTING}
            parts = key[len("BOOGLE_PROVIDER_") :].split("_")
            if len(parts) < 2:
                continue

            source = parts[0].lower()
            setting = "_".join(parts[1:]).lower()

            if source not in config:
                config[source] = {"enabled": False, "priority": 999, "config": {}}

            if setting == "enabled":
                config[source]["enabled"] = value.lower() in ("1", "true", "yes", "on")
            elif setting == "priority":
                try:
                    config[source]["priority"] = int(value)
                except ValueError:
                    pass

        return config

    @classmethod
    def clear(cls) -> None:
        """Clear all registered providers (useful for testing)."""
        cls._providers.clear()
        cls._provider_classes.clear()


# Convenience decorator
def register_provider(provider_class: Type[BaseBookProvider]) -> Type[BaseBookProvider]:
    """
    Decorator to register a provider class.

    Usage:
        @register_provider
        class MyProvider(BaseBookProvider):
            ...
    """
    return ProviderRegistry.register(provider_class)
