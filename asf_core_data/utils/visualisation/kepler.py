# File: asf_core_data/utils/visualisation/kepler.py
"""
Handle Kepler map configs and save maps.
"""

# ----------------------------------------------------------------------------------

import yaml

from asf_core_data import Path
from asf_core_data.config import base_config

# ----------------------------------------------------------------------------------

# Load config file
KEPLER_PATH = base_config.KEPLER_OUTPUT


def get_config(filename, data_path=".", rel_path=KEPLER_PATH):
    """Return Kepler config in yaml format.

    Args:
        filename (str): Config filename.
        data_path (str/Path, optional):  Path to config files. Defaults to ".".
        rel_path (str/Path, optional): Relative path to Kepler configs. Defaults to KEPLER_PATH.

    Returns:
        config (str): Kepler configuration content.
    """

    config_path = Path(data_path) / rel_path / "configs" / filename

    with open(config_path, "r") as infile:
        config = infile.read()
        config = yaml.load(config, Loader=yaml.FullLoader)

    return config


def save_config(map, filename, data_path=".", rel_path=KEPLER_PATH):
    """Save Kepler map configruations.

    Args:
        map (Kepler.map):  Kepler map after modifications.
        filename (str): Config filename.
        data_path (str/Path, optional):  Path to config files. Defaults to ".".
        rel_path (str/Path, optional): Relative path to Kepler configs. Defaults to KEPLER_PATH.
    """

    config_path = Path(data_path) / rel_path / "configs" / filename

    with open(config_path, "w") as outfile:
        outfile.write(str(map.config))


def save_map(map, filename, data_path=".", rel_path=KEPLER_PATH):
    """Save Kepler map as HTML.

    Args:
        map (Kepler.map): Kepler map after modifications.
        filename (str): Map filename.
        data_path (str/Path, optional): Path to config files. Defaults to ".".
        rel_path (str/Path, optional):  Relative path to Kepler maps. Defaults to KEPLER_PATH.
    """

    map_path = Path(data_path) / rel_path / "maps" / filename
    map.save_to_html(file_name=map_path)
