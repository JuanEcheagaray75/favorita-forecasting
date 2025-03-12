from kedro_mlflow.framework.hooks import MlflowHook

from favorita.hooks import SparkHooks  # noqa: E402

# Hooks are executed in a Last-In-First-Out (LIFO) order.
HOOKS = (
    SparkHooks(),
    MlflowHook(),
)

# Installed plugins for which to disable hook auto-registration.
# DISABLE_HOOKS_FOR_PLUGINS = ("kedro-viz",)

from pathlib import Path  # noqa: E402

from kedro_viz.integrations.kedro.sqlite_store import SQLiteStore  # noqa: E402

# Class that manages storing KedroSession data.

SESSION_STORE_CLASS = SQLiteStore
# Keyword arguments to pass to the `SESSION_STORE_CLASS` constructor.
SESSION_STORE_ARGS = {"path": str(Path(__file__).parents[2])}

# Directory that holds configuration.
# CONF_SOURCE = "conf"

# Class that manages how configuration is loaded.
from kedro.config import OmegaConfigLoader  # noqa: E402

CONFIG_LOADER_CLASS = OmegaConfigLoader
# Keyword arguments to pass to the `CONFIG_LOADER_CLASS` constructor.
CONFIG_LOADER_ARGS = {
    "base_env": "base",
    "default_run_env": "local",
    "config_patterns": {
        "spark": ["spark*", "spark*/**"],
    },
}

# Class that manages Kedro's library components.
# from kedro.framework.context import KedroContext
# CONTEXT_CLASS = KedroContext

# Class that manages the Data Catalog.
# from kedro.io import DataCatalog
# DATA_CATALOG_CLASS = DataCatalog
