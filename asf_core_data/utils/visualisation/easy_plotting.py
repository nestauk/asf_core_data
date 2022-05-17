# File: asf_core_data/utils/visualisation/easy_plotting.py
"""Functions to help plot and analyse dataframe.
"""

# ---------------------------------------------------------------------------------

# Imports

import re
import warnings

warnings.simplefilter("ignore", category=UserWarning)

import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.gridspec import GridSpec

from asf_core_data import Path
from asf_core_data.utils.visualisation import feature_settings
from asf_core_data import PROJECT_DIR


# ---------------------------------------------------------------------------------


def save_figure(
    plt,
    fig_path,
    plot_title=None,
    file_extension=".png",
    dpi=1000,
):
    """Create filename and save figure.

    Args:
        plt (matplotlib.pyplot): Plot to save.
        fig_path (str): Location to save the plot.
        plot_title (str, optional): Use plot title to generate filename. If None, use "figure.png" as filename.
        file_extension (str, optional): File extension, file format to save. Defaults to ".png".
        dpi (int, optional):  Dots per inches (dpi) determines how many pixels the figure comprises. Defaults to 1000.

    """

    # Tight layout
    # plt.tight_layout()

    if fig_path is None:
        fig_path = PROJECT_DIR / "outputs/figures/"

    # Automatically generate filename
    if plot_title is not None:
        save_filename = plot_title
        save_filename = re.sub(" ", "_", save_filename)
        save_filename = save_filename + file_extension

    # Use "figure" as filename as default
    else:
        save_filename = "figure.png"

    # Save fig
    plt.savefig(Path(fig_path) / save_filename, dpi=dpi, bbox_inches="tight")


def get_readable_tick_labels(plt, ticklabel_type, axis):
    """Get more readable tick labels, e.g. 50k for 50000.

    Args:
        plt (matplotlib.pyplot): Plot from which to get axes.
        ticklabel_type (str): Label type for ticklabel (y-axis or x-axis).
            Options: '', 'm', 'k' or '%', defaults to None.
        axis (str): For which axis to update the labels. Options: 'x', 'y'.

    Returns:
        labels (list): Updated tick labels for x or y axis.
        ax (plt.gca): Current axes for plt.
        division_int (int): By which number to divide values to match new tick labels.
        division_type (str): Same as ticklabel_type, except for None (--> ""),
            representing string to display after updated tick values.
    """

    # Depending on ticklabel adjust display of numbers
    # e.g. (50000 --> 50k)

    # 1000 --> k
    if ticklabel_type == "k":
        division_type = "k"
        division_int = 1000.0

    # 1000000 --> m
    elif ticklabel_type == "m":
        division_type = "m"
        division_int = 1000000.0

    # "" or None --> ""
    elif ticklabel_type == "" or ticklabel_type is None:
        division_type = ""
        division_int = 1.0

    # % --> %
    elif ticklabel_type == "%":
        division_type = "%"
        division_int = 1.0

    else:
        raise IOError("Invalid value: ticklabel has to be '', 'm', 'k' or '%'")

    # Get axis and ticks
    ax = plt.gca()

    if axis == "y":
        ticks = ax.get_yticks()
    else:
        ticks = ax.get_xticks()

    # Get updated tick labels
    labels = ["{:.0f}".format(x / division_int) + division_type for x in ticks]

    return labels, ax, division_int, division_type


def plot_subcategory_distribution(
    df,
    category,
    fig_save_path,
    normalize=False,
    color="lightseagreen",
    plot_title=None,
    y_label="",
    x_label="",
    y_ticklabel_type=None,
    x_tick_rotation=0,
):
    """Plot distribution of subcategories/values of specific category/feature.

    Args:
        df (pd.DataFrame): Dataframe to analyse and plot.
        category (str): Category/column of interest for which distribution is plotted.
        fig_save_path (str): Location where to save plot.
        normalize (bool, optional): If True, relative numbers (percentage) instead of absolute numbers.
            Defaults to False.
        color (str, optional): Color of bars in plot. Defaults to "lightseagreen".
        plot_title (str, optional): Title to display above plot.
            If None, title is created automatically.
            Plot title is also used when saving file. Defaults to None.
        y_label (str, optional): Label for y-axis. Defaults to "".
        x_label (str, optional): Label for x-axis. Defaults to "".
        y_ticklabel_type (str, optional): Label for yticklabel, e.g. 'k' when displaying numbers
            in more compact way for easier readability (50000 --> 50k).
            Options: {'', 'm', 'k' or '%'}. Defaults to None.
        x_tick_rotation (int, optional): Rotation of x-tick labels.
            If rotation set to 45, make end of label align with tick (ha="right"). Defaults to 0.
    """

    # Get relative numbers (percentage) instead of absolute numbers
    if normalize:

        # Get counts for every category
        category_counts = round(
            df[category].value_counts(dropna=False, normalize=True) * 100, 2
        )
        y_ticklabel_type = "%"

    else:
        # Get counts for every category
        category_counts = df[category].value_counts(dropna=False)

    # Plot category counts
    category_counts.plot(kind="bar", color=color)

    # Set plot title
    if plot_title is None:
        plot_title = "Distribution of {}".format(category.lower())
        if normalize:
            plot_title += " (%)"

    # Add titles and labels
    plt.title(plot_title)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.xticks(
        rotation=x_tick_rotation, ha="right"
    ) if x_tick_rotation == 45 else plt.xticks(rotation=x_tick_rotation)

    # Get new yticklabels
    ax = plt.gca()

    yticklabels, ax, division_int, division_type = get_readable_tick_labels(
        plt, y_ticklabel_type, "y"
    )
    ax.set_yticklabels(yticklabels)

    # Adjust ylim in case of ylabel adjustment for easier readiablity (50000 --> 50k)
    highest_count = max(category_counts)

    for i, cty in enumerate(category_counts.values):
        ax.text(
            i,
            cty + highest_count / 80,
            str(round(cty / division_int), 1) + division_type,
            horizontalalignment="center",
        )

    ax.set_ylim([0.0, int(highest_count + highest_count / 8)])

    # Save figure
    save_figure(plt, plot_title, fig_path=fig_save_path)

    # Show plot
    plt.show()


def plot_feature_by_subcategories(
    df,
    feature_of_interest,
    category,
    fig_save_path,
    subcategory=None,
    plot_title=None,
    y_label="",
    x_label="",
    plot_kind="hist",
    x_tick_rotation=0,
):
    """Plot a feature/column by another feature/column's subcategories.
     For example, plot the energy efficiency (feature of interest) on y-axis
     by different tenure types (category) or specific tenure type (subcategory) on x-axis.

    Args:
        df (pd.DataFrame): Dataframe to analyse and plot.
        feature_of_interest (str): Feature to plot on y-axis.
        category (str): Category to plot on x-axis. Show all subcategories/values.
        fig_save_path (str): Location where to save plot.
        subcategory (str, optional): Only plot subcategories/values of given subcategory.. Defaults to None.
        plot_title (str, optional):  Type of plot: "hist", "bar". Defaults to None.
        y_label (str, optional):  Label for y-axis. Defaults to "".
        x_label (str, optional):  Label for x-axis. Defaults to "".
        plot_kind (str, optional): Type of plot: "hist", "bar". Defaults to "hist".
        x_tick_rotation (int, optional): Rotation of x-tick labels.
            If rotation set to 45, make end of label align with tick (ha="right"). Defaults to 0.

    """

    # Tag for title
    tag = ""

    # Load data for specific subcategory (if given)
    if subcategory is not None:
        df = df.loc[df[category] == subcategory]
        tag = " for " + str(subcategory)

    # Create plot title
    if plot_title is None:
        plot_title = feature_of_interest + tag

    # Plot distribution for feature values/subcategories
    ratings = df[feature_of_interest].value_counts(dropna=False).sort_index()

    # Plot histogram with 30 bins
    if plot_kind == "hist":
        ratings.plot(kind=plot_kind, bins=30, color="lightseagreen")

    # Plot bar plot (or other types)
    else:
        ratings.plot(kind=plot_kind, color="lightseagreen")

    # Describe plot with title and labels
    plt.title(plot_title)
    plt.ylabel(y_label)
    plt.xlabel(x_label)
    plt.xticks(
        rotation=x_tick_rotation, ha="right"
    ) if x_tick_rotation == 45 else plt.xticks(rotation=x_tick_rotation)

    # Save figure
    save_figure(plt, plot_title, fig_path=fig_save_path)

    # Show plot
    plt.show()


def plot_subcats_by_other_subcats(
    df,
    feature_1,
    feature_2,
    fig_save_path,
    feature_1_order=None,
    feature_2_order=None,
    normalize=True,
    plot_title=None,
    y_label="",
    x_label="",
    plot_kind="bar",
    plotting_colors=None,
    y_ticklabel_type=None,
    x_tick_rotation=0,
    legend_loc="inside",
    with_labels=False,
    width=0.8,
    figsize=None,
):
    """Plot subcategories of given feature by subcategories of another feature.
     For example, plot and color-code the distribution of heating types (feature 2)
     on the different tenure types (feature 1).

    Args:
        df (pd.DataFrame): Dataframe to analyse and plot.
        feature_1 (str):  Feature for which subcategories are plotted on x-axis.
        feature_2 (str): Feature for which distribution is shown split
            per subcategory of feature 1. Feature 2 subcategories are represented
            with differnet colors, explained with a color legend.
        fig_save_path (str): Location where to save plot.
        feature_1_order (str, optional): The order in which feature 1 subcategories are displayed.
            Defaults to None.
        feature_2_order (str, optional): The order in which feature 2 subcategories are displayed.
            Defaults to None.
        normalize (bool, optional): If True, relative numbers (percentage) instead of absolute numbers. Defaults to None.
        plot_title (str, optional):  Title to display above plot.
            If None, title is created automatically.
            Plot title is also used when saving file. Defaults to None.
        y_label (str, optional):  Label for y-axis. Defaults to "".
        x_label (str, optional):  Label for x-axis. Defaults to "".
        plot_kind (str, optional): Type of plot: "hist", "bar". Defaults to "hist".
        plotting_colors (str/list, optional): Ordered list of colors or color map to use when plotting feature 2.
            If list, use list of colors.
            If str, use corresponding matplotlib color map.
            If None, use default color list. Defaults to None.
        y_ticklabel_type (str, optional): Label for yticklabel, e.g. 'k' when displaying numbers
            in more compact way for easier readability (50000 --> 50k).
            Options: {'', 'm', 'k' or '%'}. Defaults to None.
        x_tick_rotation (int, optional): Rotation of x-tick labels.
            If rotation set to 45, make end of label align with tick (ha="right"). Defaults to 0.
        legend_loc (str, optional): "inside", "outside" or in the "middle" of the plot box.
            Defaults to "inside".
        with_labels (bool, optional): Add labels to plot. Defaults to False.
        width (float, optional): Bar width. Defaults to 0.8.
        figsize (tuple, optional): Fig size of the plot. Defaults to None.

    """

    # Get set of values/subcategories for features.
    feature_1_values = list(set(df[feature_1].sort_index()))
    feature_2_values = list(set(df[feature_2].sort_index()))

    # Set order for feature 1 values/subcategories
    if feature_1_order is not None:
        feature_1_values = feature_1_order
    else:

        if feature_1 in feature_settings.map_dict.keys():
            feature_1_values = feature_settings.map_dict[feature_1]

            for value in feature_1_values:
                if value not in df[feature_1].unique():
                    feature_1_values.remove(value)

    # Create a feature-bar dict
    feat_bar_dict = {}

    # Get totals for noramlisation
    totals = df[feature_1].value_counts(dropna=False)

    # For every feature 2 value/subcategory, get feature 1 values
    # e.g. for every tenure type, get windows energy efficiencies
    for feat2 in feature_2_values:
        dataset_of_interest = df.loc[df[feature_2] == feat2][feature_1]
        data_of_interest = dataset_of_interest.value_counts(dropna=False)

        if normalize:
            feat_bar_dict[feat2] = data_of_interest / totals * 100
        else:
            feat_bar_dict[feat2] = data_of_interest

    # Save feature 2 subcategories by feature 1 subcategories as dataframe
    subcat_by_subcat = pd.DataFrame(feat_bar_dict, index=feature_1_values)

    # If feature 2 order is given, rearrange
    if feature_2_order is not None:
        subcat_by_subcat = subcat_by_subcat[feature_2_order]
    else:

        if feature_2 in feature_settings.map_dict.keys():
            feature_2_values = feature_settings.map_dict[feature_2]
            for value in feature_2_values:
                if value not in df[feature_2].unique():
                    feature_2_values.remove(value)

            subcat_by_subcat = subcat_by_subcat[feature_2_values]

    # If not defined, set default colors for plotting
    if plotting_colors is None:
        plotting_colors = ["green", "greenyellow", "yellow", "orange", "red"]

    # Use given colormap
    if isinstance(plotting_colors, str):
        cmap = plotting_colors
        subcat_by_subcat.plot(
            kind=plot_kind, cmap=cmap, width=width
        )  # recommended RdYlGn

    # or: use given color list
    elif isinstance(plotting_colors, list):
        subcat_by_subcat.plot(kind=plot_kind, color=plotting_colors, width=width)

    else:
        raise IOError("Invalid plotting_colors '{}'.".format(plotting_colors))

    # Adjust figsize
    if figsize is not None:
        fig = plt.gcf()
        fig.set_size_inches(figsize[0], figsize[1])

    # Get updated yticklabels
    ax = plt.gca()
    yticklabels, ax, _, _ = get_readable_tick_labels(plt, y_ticklabel_type, "y")
    ax.set_yticklabels(yticklabels)

    # Set plot title
    if plot_title is None:
        plot_title = feature_2 + " by " + feature_1

    # Describe plot with title and axes
    plt.title(plot_title)
    plt.ylabel(y_label)
    plt.xlabel(x_label)
    plt.xticks(
        rotation=x_tick_rotation, ha="right"
    ) if x_tick_rotation == 45 else plt.xticks(rotation=x_tick_rotation)

    if legend_loc == "outside":
        leg = plt.legend(bbox_to_anchor=(1.05, 1), loc="upper left")
        leg.set_draggable(state=True)

    if legend_loc == "middle":
        leg = plt.legend(bbox_to_anchor=(1.05, 1), loc="upper center")

        leg.set_draggable(state=True)

    if with_labels:
        labels = subcat_by_subcat.T.fillna(0.0).to_numpy().flatten()

        labels = [str(round(l)) + "%" for l in labels]
        rects = ax.patches

        for rect, label in zip(rects, labels):
            height = rect.get_height()
            ax.text(
                rect.get_x() + rect.get_width() / 2,
                height + 0.5,
                label,
                ha="center",
                va="bottom",
                fontsize=8,
            )

    # Save figure
    save_figure(plt, plot_title, fig_path=fig_save_path)

    # Show plot
    plt.show()


def plot_correlation(
    df,
    feature_1,
    feature_2,
    fig_save_path,
    with_hist_subplots=True,
    ylim_max=100,
    plot_title=None,
    y_label="",
    x_label="",
    x_tick_rotation=0,
):
    """Plot the correlation of two features.

    Args:
        df (pandas.DataFrame): Dataframe which holds features for which to plot correlation.
        feature_1 (str): Feature to plot on x-axis.
        feature_2 (str): Feature to plot on y-axis.
        fig_save_path (str): Location where to save plot.
        with_hist_subplots (bool, optional): Plot histogram subplots above and besides correlation plot
            for both features. Defaults to True.
        ylim_max (int, optional):  Limit for y-axis for better readbility. Defaults to 100.
        plot_title (_type_, optional): Title to display above plot.
            If None, title is created automatically.
            Plot title is also used when saving file. Defaults to None.
        x_tick_rotation (int, optional): Rotation of x-tick labels.
            If rotation set to 45, make end of label align with tick (ha="right"). Defaults to 0.
    """

    # Set plot title
    tag = " with hist subplots" if with_hist_subplots else ""

    if plot_title is None:
        plot_title = "Correlation " + feature_2 + " by " + feature_1 + tag

    # With subplots with histogram on sides
    if with_hist_subplots:

        # Set figure size
        fig = plt.figure(figsize=(8.5, 6.0))

        # Set GridSpec
        gs = GridSpec(4, 4)

        # Add scatter subplot
        ax_scatter = fig.add_subplot(gs[1:4, 0:3])

        # Set title and labels
        plt.title(plot_title)
        plt.xlabel(feature_1)
        plt.ylabel(feature_2)
        plt.xticks(
            rotation=x_tick_rotation, ha="right"
        ) if x_tick_rotation == 45 else plt.xticks(rotation=x_tick_rotation)

        # Set Histogram subplot for feature 1
        ax_hist_x = fig.add_subplot(gs[0, 0:3], title=feature_1 + " Histogram")

        # Set Histogram subplot for feature 2
        ax_hist_y = fig.add_subplot(gs[1:4, 3])

        # Adjust position of label
        ax_hist_y.yaxis.set_label_position("right")
        ax_hist_y.set_ylabel(feature_2 + " Histogram", rotation=270, va="bottom")

        # Scatter plot for feature 1 and feature 2
        ax_scatter.scatter(df[feature_1], df[feature_2], alpha=0.1, s=5)

        # Add histogram for feature 1 and feature 2
        ax_hist_x.hist(df[feature_1])
        ax_hist_y.hist(df[feature_2], orientation="horizontal")

        # Set ylim max for all subplots with feature 1
        ax_hist_y.set_ylim([0.0, ylim_max])
        ax_scatter.set_ylim([0.0, ylim_max])

    else:

        # Create scatter flot with feature 1 and feature 2
        plt.scatter(df[feature_1], df[feature_2], alpha=0.1, s=2)

        # Set ylim
        ax = plt.gca()
        ax.set_ylim([0.0, ylim_max])

        # Set labels
        plt.xlabel(feature_1)  # or x_label
        plt.ylabel(feature_2)  # or y_label
        plt.xticks(
            rotation=x_tick_rotation, ha="right"
        ) if x_tick_rotation == 45 else plt.xticks(rotation=x_tick_rotation)

    # Save figure
    save_figure(plt, plot_title, fig_save=fig_save_path)

    # Show plot
    plt.show()
