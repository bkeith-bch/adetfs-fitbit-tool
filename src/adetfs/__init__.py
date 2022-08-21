from importlib import resources
try:
    import tomllib
except ModuleNotFoundError:
    import toml as tomllib

# Version of the adetfs package
__version__ = "1.1.rc0"

_cfg = tomllib.loads(resources.read_text("adetfs", "config.toml"))
URL = _cfg["homepage"]["url"]