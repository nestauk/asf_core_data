# File: heat_pump_adoption_modelling/pipeline/supervised_model/utils/kepler.py
"""
Generate Kepler maps for supervised model outputs.
"""

# ----------------------------------------------------------------------------------


import yaml
from asf_core_data import get_yaml_config, Path, PROJECT_DIR
from asf_core_data.config import base_config


# Load config file
KEPLER_PATH = base_config.KEPLER_OUTPUT


def get_config(filename, data_path=".", rel_path=KEPLER_PATH):
    """Return Kepler config in yaml format.

    Parameters
    ----------
    path: str
        Path to config files.

    Return
    ---------
    config: str
        Return Kepler configuration content."""

    config_path = Path(data_path) / rel_path / "configs" / filename

    with open(config_path, "r") as infile:
        config = infile.read()
        config = yaml.load(config, Loader=yaml.FullLoader)

    return config


def save_config(map, filename, data_path=".", rel_path=KEPLER_PATH):
    """Save Kepler map configruations.

    Parameters
    ----------
    map: Kepler.map
        Kepler map after modifications.

    config_path: str
        Path to config files.

    Return: None"""

    config_path = Path(data_path) / rel_path / "configs" / filename

    with open(config_path, "w") as outfile:
        outfile.write(str(map.config))


def save_map(map, filename, data_path=".", rel_path=KEPLER_PATH):
    """Save Kepler map as HTML.

    Parameters
    ----------
    map: Kepler.map
        Kepler map after modifications.

    config_path: str
        Path to config files.

    Return: None"""

    map_path = Path(data_path) / rel_path / "maps" / filename
    map.save_to_html(file_name=map_path)
