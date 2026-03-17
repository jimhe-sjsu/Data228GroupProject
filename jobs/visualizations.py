import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns

sns.set_theme(style="whitegrid", palette="muted", font_scale=1.1)

def _ensure_dir(path):
    os.makedirs(path, exist_ok=True)

def plot_null_counts(null_dict, output_dir):
    _ensure_dir(output_dir)
    filepath = os.path.join(output_dir, "null_counts.png")

    data = {k: v for k, v in null_dict.items() if v > 0}
    if not data:
        return filepath

    cols = sorted(data, key=data.get, reverse=True)
    values = [data[c] for c in cols]

    fig, ax = plt.subplots(figsize=(10, max(4, len(cols) * 0.45)))
    ax.barh(cols, values, color=sns.color_palette("Reds_r", len(cols)))
    ax.set_xlabel("Null Count")
    ax.set_title("Null Values per Column (Before Cleaning)")
    ax.invert_yaxis()
    for i, v in enumerate(values):
        ax.text(v + max(values) * 0.01, i, f"{v:,}", va="center", fontsize=9)
    fig.tight_layout()
    fig.savefig(filepath, dpi=150)
    plt.close(fig)
    return filepath

def plot_row_counts_by_step(step_log, output_dir):
    _ensure_dir(output_dir)
    filepath = os.path.join(output_dir, "row_counts_by_step.png")

    labels = ["Raw"] + [s["step"] for s in step_log]
    counts = [step_log[0]["before"]] + [s["after"] for s in step_log]

    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(labels, counts, marker="o", linewidth=2, color="#2c7fb8")
    ax.fill_between(range(len(counts)), counts, alpha=0.15, color="#2c7fb8")
    ax.set_ylabel("Row Count")
    ax.set_title("Row Count After Each Cleaning Step")
    plt.xticks(rotation=35, ha="right")
    for i, c in enumerate(counts):
        ax.annotate(f"{c:,}", (i, c), textcoords="offset points", xytext=(0, 10), ha="center", fontsize=8)
    fig.tight_layout()
    fig.savefig(filepath, dpi=150)
    plt.close(fig)
    return filepath

def plot_trip_distance_histogram(before_sample, after_sample, output_dir):
    _ensure_dir(output_dir)
    filepath = os.path.join(output_dir, "trip_distance_before_after.png")

    fig, axes = plt.subplots(1, 2, figsize=(14, 5), sharey=False)

    axes[0].hist(before_sample["trip_distance"].dropna(), bins=100, color="#d95f02", edgecolor="white")
    axes[0].set_title("Trip Distance - Before Cleaning")
    axes[0].set_xlabel("Miles")
    axes[0].set_ylabel("Frequency")

    axes[1].hist(after_sample["trip_distance"].dropna(), bins=100, color="#1b9e77", edgecolor="white")
    axes[1].set_title("Trip Distance - After Cleaning")
    axes[1].set_xlabel("Miles")

    fig.suptitle("Trip Distance Distribution", fontsize=14, y=1.02)
    fig.tight_layout()
    fig.savefig(filepath, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return filepath

def plot_fare_distribution(before_sample, after_sample, output_dir):
    _ensure_dir(output_dir)
    filepath = os.path.join(output_dir, "fare_distribution_before_after.png")

    fig, axes = plt.subplots(1, 2, figsize=(14, 5), sharey=False)

    axes[0].hist(before_sample["fare_amount"].dropna(), bins=100, color="#d95f02", edgecolor="white")
    axes[0].set_title("Fare Amount - Before Cleaning")
    axes[0].set_xlabel("USD")
    axes[0].set_ylabel("Frequency")

    axes[1].hist(after_sample["fare_amount"].dropna(), bins=100, color="#1b9e77", edgecolor="white")
    axes[1].set_title("Fare Amount - After Cleaning")
    axes[1].set_xlabel("USD")

    fig.suptitle("Fare Distribution", fontsize=14, y=1.02)
    fig.tight_layout()
    fig.savefig(filepath, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return filepath
