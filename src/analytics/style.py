import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap
from matplotlib.ticker import FuncFormatter

from src.utils.config import PROJECT_ROOT

FIGURES_DIR = PROJECT_ROOT / "reports" / "figures"

SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
BLUE = SERIES[0]
ORANGE = SERIES[1]
CONTEXT = "#c3c2b7"
BLUE_RAMP = ["#cde2fb", "#b7d3f6", "#9ec5f4", "#86b6ef", "#6da7ec", "#5598e7", "#3987e5",
             "#2a78d6", "#256abf", "#1c5cab", "#184f95", "#104281", "#0d366b"]
DIVERGING_MID = "#f0efec"
RED = "#e34948"

SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_2 = "#52514e"
MUTED = "#898781"
GRID = "#e1e0d9"
AXIS = "#c3c2b7"

SEQUENTIAL = LinearSegmentedColormap.from_list("blue_seq", BLUE_RAMP)
DIVERGING = LinearSegmentedColormap.from_list("blue_red", [BLUE_RAMP[10], BLUE_RAMP[3], DIVERGING_MID, "#f0a3a2", RED])


def apply_style() -> None:
    mpl.rcParams.update({
        "figure.facecolor": SURFACE,
        "axes.facecolor": SURFACE,
        "savefig.facecolor": SURFACE,
        "figure.dpi": 110,
        "savefig.dpi": 150,
        "figure.figsize": (10, 4.2),
        "font.family": ["Segoe UI", "DejaVu Sans", "sans-serif"],
        "font.size": 10,
        "text.color": INK,
        "axes.labelcolor": INK_2,
        "axes.edgecolor": AXIS,
        "axes.linewidth": 0.8,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.spines.left": False,
        "axes.grid": True,
        "axes.grid.axis": "y",
        "axes.axisbelow": True,
        "axes.titlesize": 12,
        "axes.titleweight": "semibold",
        "axes.titlelocation": "left",
        "axes.titlepad": 22,
        "axes.prop_cycle": mpl.cycler(color=SERIES),
        "grid.color": GRID,
        "grid.linewidth": 0.7,
        "grid.linestyle": "-",
        "xtick.color": MUTED,
        "ytick.color": MUTED,
        "xtick.labelcolor": INK_2,
        "ytick.labelcolor": INK_2,
        "xtick.major.size": 0,
        "ytick.major.size": 0,
        "lines.linewidth": 2,
        "lines.markersize": 5,
        "legend.frameon": False,
        "legend.fontsize": 9,
        "legend.labelcolor": INK_2,
    })


WEEKDAYS_SHORT = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
WEEKDAYS = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]


def num(x: float, decimals: int = 0) -> str:
    return f"{x:,.{decimals}f}".replace(",", " ").replace(".", ",")


def compact(x: float, prefix: str = "") -> str:
    sign = "-" if x < 0 else ""
    x = abs(x)
    for limit, suffix in ((1e9, " млрд"), (1e6, " млн"), (1e3, " тыс.")):
        if x >= limit:
            v = x / limit
            text = f"{v:.1f}".removesuffix(".0").replace(".", ",") if v < 100 else f"{v:.0f}"
            return f"{sign}{prefix}{text}{suffix}"
    return f"{sign}{prefix}{num(x)}"


def brl(x: float) -> str:
    return compact(x, "R$ ")


def money(x: float, decimals: int = 0) -> str:
    return f"R$ {num(x, decimals)}"


def pct(x: float, decimals: int = 0) -> str:
    return f"{num(x * 100, decimals)}%"


def as_pct(df, columns, decimals: int = 1):
    out = df.copy()
    for c in columns:
        out[c] = out[c].map(lambda v: pct(v, decimals) if v == v and v is not None else "")
    return out


def money_axis(ax, axis: str = "y", full: bool = False) -> None:
    fmt = FuncFormatter(lambda v, _: money(v) if full else brl(v))
    (ax.yaxis if axis == "y" else ax.xaxis).set_major_formatter(fmt)


def percent_axis(ax, axis: str = "y", decimals: int = 0) -> None:
    fmt = FuncFormatter(lambda v, _: pct(v, decimals))
    (ax.yaxis if axis == "y" else ax.xaxis).set_major_formatter(fmt)


def count_axis(ax, axis: str = "y") -> None:
    fmt = FuncFormatter(lambda v, _: num(v))
    (ax.yaxis if axis == "y" else ax.xaxis).set_major_formatter(fmt)


def titles(ax, title: str, subtitle: str | None = None) -> None:
    ax.set_title(title, loc="left")
    if subtitle:
        ax.text(0, 1.02, subtitle, transform=ax.transAxes, fontsize=9, color=INK_2, va="bottom")


def hbar(ax, labels, values, highlight=None, color: str = BLUE, fmt=None) -> None:
    labels, values = list(labels)[::-1], list(values)[::-1]
    highlight = set(highlight) if highlight is not None else set()
    colors = [color if not highlight or lab in highlight else CONTEXT for lab in labels]
    ax.barh(labels, values, color=colors, height=0.62, edgecolor=SURFACE, linewidth=2)
    ax.grid(axis="y", visible=False)
    ax.grid(axis="x", visible=True)
    ax.spines["bottom"].set_visible(False)
    if fmt is not None:
        top = max(range(len(values)), key=lambda i: values[i])
        ax.text(values[top], top, "  " + fmt(values[top]), va="center", fontsize=9, color=INK)


def label_point(ax, x, y, text: str, dy: int = 8, ha: str = "center", color: str = INK) -> None:
    ax.annotate(text, (x, y), xytext=(0, dy), textcoords="offset points", ha=ha, fontsize=9, color=color)


def save(fig, name: str) -> None:
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    fig.savefig(FIGURES_DIR / f"{name}.png", bbox_inches="tight")
