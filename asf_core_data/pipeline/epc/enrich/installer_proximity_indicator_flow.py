"""In activated conda environment, python installer_proximity_indicator_flow.py run"""

############################################################
from typing import Dict, Generator, List

from metaflow import current, FlowSpec, step, Parameter, project, S3

from asf_core_data import config

############################################################


@project(name="installer_proximity_indicator")
class InstallerProximity(FlowSpec):
    """Generates installer proximity score for houses in EPC England and Wales data.

    Steps:
    - Load MCS installer and  installations data; uk postcodes to latlong and epc data.
    - Generate MCS installer company buffers based on installation
    distance spread.
    - Identify houses within company buffers and calculate weighted
    installer proximity score per EPC building.
    - Save EPC data with associated installer proximity score.
    """

    clean_installations_path = Parameter(
        "clean-installations",
        help="The s3 path to MCS clean installations data",
        type=str,
        default=config["PREPROC_GEO_MCS_INSTALLATIONS_PATH"],
    )

    clean_installers_path = Parameter(
        "clean-installers",
        help="The s3 path to MCS clean installer company data",
        type=str,
        default=config["PREPROC_MCS_INSTALLER_COMPANY_PATH"],
    )

    epc_path = Parameter(
        "epc-s3",
        help="the s3 path to clean EPC data",
        type=str,
        default=config["PREPROC_EPC_DATA_SAMPLE_PATH"],
    )

    uk_geo_path = Parameter(
        "uk-geo",
        help="the s3 path to UK postcodes to latlong data",
        type=str,
        default=config["UK_GEO_PATH"],
    )

    epc_installer_prox_path = Parameter(
        "epc-installer-prox-path",
        help="the s3 path for epc data with installer proximity scores.",
        type=str,
        default=config["PREPROC_EPC_DATA_SAMPLE_PROX_PATH"],
    )

    epc_installer_prox_name = Parameter(
        "epc-installer-prox-name",
        help="the s3 name for epc data with installer proximity scores.",
        type=str,
        default=config["PREPROC_EPC_DATA_SAMPLE_PROX_NAME"],
    )

    @step
    def start(self):
        """Load relevant data."""
        from asf_core_data.getters.data_getters import load_s3_data, s3
        from asf_core_data import bucket_name

        self.clean_installations = load_s3_data(
            s3, bucket_name, self.clean_installations_path
        )
        self.clean_installers = load_s3_data(
            s3, bucket_name, self.clean_installers_path
        )
        # load sample
        self.epc_data = load_s3_data(s3, bucket_name, self.epc_path)
        self.postcodes_geo_data = load_s3_data(s3, bucket_name, self.uk_geo_path)

        print("loaded relevant datasets!")

        self.next(self.generate_buffers)

    @step
    def generate_buffers(self):
        """Generate MCS company installer buffers based on installation
        distance spread."""
        from asf_core_data.pipeline.epc.enrich.installer_proximity_indicator_utils import (
            generate_distance_spread_buffers,
        )

        self.company_buffers = generate_distance_spread_buffers(
            self.clean_installations, self.clean_installers
        )

        print("generated company buffers based on installation distance spread!")

        self.next(self.geocode_epc)

    @step
    def geocode_epc(self):
        """Geocode EPC data."""
        import geopandas as gpd
        import pandas as pd

        self.epc_geo_data = pd.merge(
            self.epc_data,
            self.postcodes_geo_data,
            left_on="POSTCODE",
            right_on="postcode",
        )

        self.epc_geo_data = gpd.GeoDataFrame(
            self.epc_geo_data,
            geometry=gpd.points_from_xy(
                self.epc_geo_data.longitude, self.epc_geo_data.latitude
            ),
        )

        print(
            "lost ",
            len(self.epc_data) - len(self.epc_geo_data),
            " records when geocoding.",
        )

        self.next(self.calculate_installer_proximity)

    @step
    def calculate_installer_proximity(self):
        """Calculate installer proximity in preprocessed, deduped EPC data."""

        from asf_core_data.pipeline.epc.enrich.installer_proximity_indicator_utils import (
            calculate_buffer_distribution,
            calculate_weighted_installer_proximity,
        )

        self.proximity_scores = []
        for epc_geo in self.epc_geo_data["geometry"].values:
            buffer_distbs = calculate_buffer_distribution(self.company_buffers, epc_geo)
            weighted_installer_prox = calculate_weighted_installer_proximity(
                buffer_distbs
            )
            self.proximity_scores.append(weighted_installer_prox)

        self.epc_geo_data["installer_proximity_score"] = self.proximity_scores

        print("calculated installer proximity scores!")

        self.next(self.end)

    @step
    def end(self):
        """Dump EPC data with geo and indicator to outputs."""
        from asf_core_data import bucket_name

        with S3(s3root="s3://" + bucket_name + self.epc_installer_prox_path) as s3:
            self.epc_geo_data.to_csv(
                "s3://"
                + bucket_name
                + self.epc_installer_prox_path
                + self.epc_installer_prox_name,
                index=False,
            )

        print(
            "successfully saved data to: ",
            "s3://"
            + bucket_name
            + self.epc_installer_prox_path
            + self.epc_installer_prox_name,
        )


if __name__ == "__main__":
    InstallerProximity()
