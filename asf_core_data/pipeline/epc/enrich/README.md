### Enrich pipeline

Welcome to the enrich pipeline of EPC data in our core data infrastructure! The theory here is that we could store data enriching processes like indicators that relate to EPC data.

#### Installer Proximity Indicator

This enrich pipeline generates scores to indicate whether a given home in EPC data is in close proximity to MCS-registered installer companies. A higher score means that a given home in EPC data is in "perfect" proximity while a lower score indicates no proximity.

The indicator works as follows:

1. **Generate buffers based on HP installations distances distribution per MCS-registered HP company.** Five buffers per HP company are generated: a minimum buffer between the HP company installation centroid and the closest install; a 25 buffer between the HP company installation centroid and the 25% install distance; a 50 buffer between the HP company installation centroid and the 50% install distance; a 75 buffer between the HP company installation centroid and the 70% install distance and finally, a max buffer between the HP company installation median point and the max install distance. If a HP company has more than 1 installation, the central point is the median point. If the HP company has only installed 1 HP, the central point is the installer company location.

2. **Calculate the number of different company buffers the EPC home falls within.** This step ends up with a count of how many min, 25, 50, 75 and max company buffers a given EPC home falls within.

3. **Generate indicator score by weighing company buffer counts per EPC home.** Where the minimum buffer count weights more than the maximum buffer count. If a given EPC home falls within many minimum company buffers, the home will have a high proximity indicator score while the opposite is true if the home falls within many maximum company buffers.

<img width="814" alt="indicator_visual" src="https://user-images.githubusercontent.com/46863334/166961845-b5faebef-6e8e-4338-a2fc-817f01669d91.png">

To generate installer proximity scores on a sample of 50,000 EPC ratings from EST cleansed, deduplicated EPC data for England and Wales, run `python installer_proximity_indicator_flow.py run` in your activated conda environment.
