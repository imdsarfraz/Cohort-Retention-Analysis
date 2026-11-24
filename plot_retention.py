"""Draw the cohort retention heatmap shown in the README.

Reads "Cohort Retention Data.csv" (the output of the last query in Cohort.sql) and saves
images/cohort_retention_heatmap.png, plus a _dark version for GitHub's dark theme.

    pip install pandas matplotlib
    python plot_retention.py
"""
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.colors import LinearSegmentedColormap, Normalize, to_rgb

HERE = Path(__file__).parent
PARTIAL_MONTH = pd.Period("2011-12", "M")  # the data stops on 9 Dec 2011

# One blue scale, light -> dark: the more customers came back, the darker the cell
BLUES = ["#cde2fb", "#b7d3f6", "#9ec5f4", "#86b6ef", "#6da7ec", "#5598e7", "#3987e5",
         "#2a78d6", "#256abf", "#1c5cab", "#184f95", "#104281", "#0d366b"]
THEMES = {
    "light": dict(surface="#fcfcfb", ink="#0b0b0b", muted="#52514e", neutral="#e1e0d9", ramp=BLUES),
    # on a dark page the scale is flipped, so low values blend into the background there too
    "dark": dict(surface="#1a1a19", ink="#f0efec", muted="#c3c2b7", neutral="#383835", ramp=BLUES[::-1]),
}

plt.rcParams["font.family"] = "sans-serif"
plt.rcParams["font.sans-serif"] = ["Segoe UI", "Helvetica", "Arial", "DejaVu Sans"]

df = pd.read_csv(HERE / "Cohort Retention Data.csv", index_col="cohort_month")
cohorts = pd.PeriodIndex(df.index, freq="M")
rates = df.filter(like="month_").to_numpy()  # rows = cohorts, column j = month_j (months since first purchase)
n_rows, n_cols = rates.shape


def luminance(color):
    """Relative luminance (WCAG) of a colour, from 0 (black) to 1 (white)."""
    rgb = np.array(to_rgb(color))
    rgb = np.where(rgb <= 0.04045, rgb / 12.92, ((rgb + 0.055) / 1.055) ** 2.4)
    return rgb @ [0.2126, 0.7152, 0.0722]


def draw(theme):
    t = THEMES[theme]
    cmap = LinearSegmentedColormap.from_list("ramp", t["ramp"])
    norm = Normalize(vmin=0, vmax=50)

    fig = plt.figure(figsize=(8.6, 5.9), dpi=200, facecolor=t["surface"])
    ax = fig.add_axes([0.13, 0.15, 0.76, 0.68], facecolor=t["surface"])

    for i in range(n_rows):
        for j in range(n_cols):
            value = rates[i, j]
            if np.isnan(value):  # the cohort hasn't reached this month yet
                continue
            # month 0 is 100% by definition, so it gets a neutral grey instead of a colour from the scale
            fill = t["neutral"] if j == 0 else cmap(norm(value))
            ax.add_patch(plt.Rectangle((j + 0.03, i + 0.05), 0.94, 0.90, facecolor=fill, linewidth=0))
            label = f"{value:.0f}" if j == 0 else f"{value:.1f}"
            if j > 0 and cohorts[i] + j == PARTIAL_MONTH:
                label += "*"
            if j == 0:
                text = t["ink"]
            else:  # dark text on light cells, white text on dark cells
                text = "#0b0b0b" if luminance(fill) > 0.18 else "#ffffff"
            ax.text(j + 0.5, i + 0.52, label, ha="center", va="center", fontsize=7.5, color=text)

    # cohort size to the right of each row
    for i, n in enumerate(df["customers"]):
        ax.text(n_cols + 0.25, i + 0.52, f"{n:,}", ha="left", va="center", fontsize=8, color=t["muted"])
    ax.text(n_cols + 0.25, -0.33, "Customers", ha="left", va="center", fontsize=8, color=t["muted"])

    ax.set_xlim(0, n_cols)
    ax.set_ylim(n_rows, 0)  # first cohort at the top
    ax.set_xticks(np.arange(n_cols) + 0.5, labels=range(n_cols))
    ax.set_yticks(np.arange(n_rows) + 0.5, labels=[c.strftime("%b %Y") for c in cohorts])
    ax.xaxis.tick_top()
    ax.xaxis.set_label_position("top")
    ax.set_xlabel("Months since first purchase", color=t["muted"], fontsize=8.5, labelpad=8)
    ax.tick_params(length=0, colors=t["muted"], labelsize=8)
    for spine in ax.spines.values():
        spine.set_visible(False)

    fig.text(0.02, 0.965, "Monthly cohort retention", fontsize=13, weight="semibold", color=t["ink"])
    fig.text(0.02, 0.925, "% of customers who placed an order, by month of first purchase (rows) "
             "and months since then (columns)", fontsize=8.5, color=t["muted"])

    cax = fig.add_axes([0.13, 0.075, 0.30, 0.022])
    bar = fig.colorbar(plt.cm.ScalarMappable(norm=norm, cmap=cmap), cax=cax, orientation="horizontal")
    bar.outline.set_visible(False)
    bar.ax.tick_params(length=0, colors=t["muted"], labelsize=7.5)
    bar.set_label("% of the cohort buying in that month", color=t["muted"], fontsize=8)
    fig.text(0.47, 0.06, "Month 0 = first purchase (100%).  Blank = not reached yet.\n"
             "* Dec 2011: the data stops on 9 Dec, so these are too low.",
             fontsize=7.5, color=t["muted"], va="bottom", linespacing=1.6)

    out = HERE / "images" / f"cohort_retention_heatmap{'_dark' if theme == 'dark' else ''}.png"
    out.parent.mkdir(exist_ok=True)
    fig.savefig(out, facecolor=t["surface"])
    plt.close(fig)
    print("saved", out)


for theme in THEMES:
    draw(theme)
