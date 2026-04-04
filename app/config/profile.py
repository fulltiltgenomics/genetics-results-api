"""
Configuration profile loader.

Set CONFIG_PROFILE environment variable to select which data profile to use.
Each profile is a package under app.config.profiles/ containing data path definitions.
"""

import os
import importlib

CONFIG_PROFILE = os.environ.get("CONFIG_PROFILE", "daly")


def load_profile_module(module_name: str):
    """Import and return a module from the active profile."""
    return importlib.import_module(f"app.config.profiles.{CONFIG_PROFILE}.{module_name}")
