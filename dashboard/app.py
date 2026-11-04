"""Tableau de bord RetailPulse (section 10 du cahier des charges).

Lit uniquement les données persistées dans PostgreSQL (sales_aggregates, orders) :
aucune agrégation métier n'est recalculée ici, à une exception près : la page « Carte
de chaleur » regroupe les commandes par ville, car sales_aggregates ne porte pas la ville.
Actualisation automatique.
"""
import base64
import json
import os
from pathlib import Path
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import altair as alt
import numpy as np
import pandas as pd
import psycopg2
import streamlit as st

REFRESH_SECONDS = int(os.getenv("REFRESH_SECONDS", "10"))
STALE_AFTER_SECONDS = int(os.getenv("STALE_AFTER_SECONDS", "600"))
SPARK_UI_URL = os.getenv("SPARK_UI_URL", "http://localhost:4040")  # ouvert depuis le navigateur
HEATMAP_HOURS = int(os.getenv("HEATMAP_HOURS", "2"))  # fenêtre de la carte de chaleur
CURRENCY = "USD"
SERIES_COLOR = "#EA6C0A"  # une seule série par graphique : une seule teinte, orange (même famille que la carte)
CHART_BACKGROUND = "#FFFFFF"
MUTED_COLOR = "#5B6672"
LINE_COLOR = "#CFD6DF"
RADIUS = "2px"

STATUS_LABELS = {
    "PAID": "Payée",
    "FAILED": "Paiement refusé",
    "CANCELLED": "Annulée",
    "CREATED": "En attente de paiement",
}
# Texte foncé sur fond très clair : lisible sans emoji, sobre, cohérent avec le thème.
STATUS_STYLES = {
    "Payée": "color: #14633F; background-color: #E3F2EA; font-weight: 600",
    "Paiement refusé": "color: #A32020; background-color: #FBE7E7; font-weight: 600",
    "Annulée": "color: #5B6672; background-color: #E8ECF1; font-weight: 600",
    "En attente de paiement": "color: #7A5200; background-color: #FDF1DA; font-weight: 600",
}
ORDERS_LIMIT = 50

# Période -> (heures affichées, taille d'une tranche, format de l'axe, libellé de la tranche).
# Les fenêtres Spark de 5 minutes sont simplement regroupées pour garder 50 à 75 points lisibles.
TIMELINE_PERIODS = {
    "6 dernières heures": (6, "5min", "%H:%M", "5 min"),
    "Dernier jour": (24, "30min", "%H:%M", "30 min"),
    "3 derniers jours": (72, "1h", "%d/%m %Hh", "1 h"),
    "Dernière semaine": (168, "3h", "%d/%m %Hh", "3 h"),
}

# Coordonnées des villes présentes dans le référentiel clients (DummyJSON ne fournit que le nom).
CITY_COORDS = {
    "Austin": (30.2672, -97.7431), "Charlotte": (35.2271, -80.8431),
    "Chicago": (41.8781, -87.6298), "Columbus": (39.9612, -82.9988),
    "Dallas": (32.7767, -96.7970), "Denver": (39.7392, -104.9903),
    "Fort Worth": (32.7555, -97.3308), "Houston": (29.7604, -95.3698),
    "Indianapolis": (39.7684, -86.1581), "Jacksonville": (30.3322, -81.6557),
    "Los Angeles": (34.0522, -118.2437), "New York": (40.7128, -74.0060),
    "Philadelphia": (39.9526, -75.1652), "Phoenix": (33.4484, -112.0740),
    "San Antonio": (29.4241, -98.4936), "San Diego": (32.7157, -117.1611),
    "San Francisco": (37.7749, -122.4194), "San Jose": (37.3382, -121.8863),
    "Seattle": (47.6062, -122.3321), "Washington": (38.9072, -77.0369),
}
US_STATES = json.loads((Path(__file__).parent / "us-states.json").read_text(encoding="utf-8"))
MAP_METRICS = {
    "Chiffre d'affaires": "revenue",
    "Nombre de commandes": "orders",
    "Taux d'annulation": "cancel_rate",
}
HEAT_COLORS = ["#FFF1DC", "#FED7A1", "#FDB05A", "#F97316", "#C2410C", "#7C2D12"]
HEAT_SIGMA = 1.3      # rayon d'étalement de chaque ville, en degrés (~140 km)
HEAT_STEP = 0.3       # pas de la grille de la carte de chaleur, en degrés
HEAT_MIN_SHARE = 0.03  # sous cette part de l'intensité maximale, la case reste transparente

QUERIES = {
    "kpis": """
        SELECT
            (SELECT coalesce(sum(total_revenue), 0)   FROM sales_aggregates) AS revenue,
            (SELECT coalesce(sum(total_quantity), 0)  FROM sales_aggregates) AS quantity,
            (SELECT count(DISTINCT category)          FROM sales_aggregates) AS categories,
            (SELECT count(*)                          FROM orders)           AS orders,
            (SELECT count(*) FROM orders WHERE status = 'PAID')              AS paid_orders,
            (SELECT max(updated_at)                   FROM sales_aggregates) AS last_aggregate
    """,
    "timeline": """
        SELECT window_start, sum(total_revenue) AS revenue, sum(order_count) AS orders
        FROM sales_aggregates
        WHERE window_start >= now() - interval '7 days'
        GROUP BY window_start ORDER BY window_start
    """,
    "categories": """
        SELECT category, sum(total_revenue) AS revenue,
               sum(total_quantity) AS quantity, sum(order_count) AS orders
        FROM sales_aggregates GROUP BY category ORDER BY revenue DESC
    """,
    "latest_aggregates": """
        SELECT window_start, window_end, category, order_count, total_quantity,
               total_revenue, updated_at
        FROM sales_aggregates ORDER BY window_start DESC, total_revenue DESC LIMIT 15
    """,
    "latest_orders": f"""
        SELECT o.order_id, o.created_at, c.first_name || ' ' || c.last_name AS customer,
               c.city, o.status, o.total_amount
        FROM orders o JOIN customers c USING (customer_id)
        ORDER BY o.created_at DESC LIMIT {ORDERS_LIMIT}
    """,
    "cities": f"""
        SELECT c.city,
               count(*)                                                        AS orders,
               count(*) FILTER (WHERE o.status = 'PAID')                       AS paid,
               count(*) FILTER (WHERE o.status = 'CANCELLED')                  AS cancelled,
               coalesce(sum(o.total_amount) FILTER (WHERE o.status = 'PAID'), 0) AS revenue
        FROM orders o JOIN customers c USING (customer_id)
        WHERE c.city IS NOT NULL AND o.created_at >= now() - interval '{HEATMAP_HOURS} hours'
        GROUP BY c.city ORDER BY revenue DESC
    """,
}


def load():
    connection = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"],
        connect_timeout=5,
    )
    try:
        with connection.cursor() as cursor:
            return {name: query(cursor, sql) for name, sql in QUERIES.items()}
    finally:
        connection.close()


def query(cursor, sql):
    cursor.execute(sql)
    df = pd.DataFrame(cursor.fetchall(), columns=[c.name for c in cursor.description])
    # NUMERIC arrive en Decimal : conversion en float pour les graphiques et tableaux.
    for column in df.columns:
        if not df.empty and isinstance(df[column].iloc[0], Decimal):
            df[column] = df[column].astype(float)
    return df


def money(value):
    return f"{value:,.2f} $".replace(",", " ")


def number(value):
    return f"{int(value):,}".replace(",", " ")


def freshness(last_aggregate, now):
    """Retourne (texte, niveau) sur la fraîcheur des données."""
    if last_aggregate is None or pd.isna(last_aggregate):
        return "Aucun agrégat encore écrit par Spark", "warning"
    age = (now - last_aggregate.to_pydatetime()).total_seconds()
    text = f"dernier agrégat écrit il y a {int(age)} s"
    if age > STALE_AFTER_SECONDS:
        return f"⚠️ Données figées : {text} (simulateur ou Spark arrêté ?)", "warning"
    return f"✅ Données à jour : {text}", "success"


def prepare_timeline(df, period, cumulative):
    """Filtre la période, regroupe les fenêtres en tranches et ajoute la colonne tracée (value)."""
    hours, freq, _, _ = TIMELINE_PERIODS[period]
    df = df.assign(revenue=df["revenue"].astype(float), orders=df["orders"].astype(float))
    df = df[df["window_start"] >= datetime.now(timezone.utc) - timedelta(hours=hours)]
    if df.empty:
        return df
    df = df.set_index("window_start")[["revenue", "orders"]].resample(freq).sum().reset_index()
    df["value"] = df["revenue"].cumsum() if cumulative else df["revenue"]
    return df


def timeline_chart(df, period, cumulative):
    _, _, axis_format, bucket = TIMELINE_PERIODS[period]
    title = f"CA cumulé ({CURRENCY})" if cumulative else f"CA par tranche de {bucket} ({CURRENCY})"
    hover = alt.selection_point(fields=["window_start"], nearest=True, on="pointerover", empty=False)
    base = alt.Chart(df).encode(
        x=alt.X("window_start:T", title=None, axis=alt.Axis(format=axis_format, grid=False),
                scale=alt.Scale(padding=8)),  # marge pour ne pas rogner les points aux extrémités
        y=alt.Y("value:Q", title=title, axis=alt.Axis(format="~s")),
    )
    tooltip = [alt.Tooltip("window_start:T", title="Début de tranche", format="%d/%m %H:%M")]
    if cumulative:
        tooltip.append(alt.Tooltip("value:Q", title=f"CA cumulé ({CURRENCY})", format=",.2f"))
    tooltip += [
        alt.Tooltip("revenue:Q", title=f"CA de la tranche ({CURRENCY})", format=",.2f"),
        alt.Tooltip("orders:Q", title="Commandes (somme par catégorie)", format=",.0f"),
    ]
    line = base.mark_line(color=SERIES_COLOR, strokeWidth=2)
    # Un point par tranche : disque orange cerclé de la couleur du fond du graphique.
    points = base.mark_circle(color=SERIES_COLOR, opacity=1, stroke=CHART_BACKGROUND, strokeWidth=2).encode(
        size=alt.condition(hover, alt.value(140), alt.value(70)),
        tooltip=tooltip,
    ).add_params(hover)
    rule = base.mark_rule(color=MUTED_COLOR).encode(
        opacity=alt.condition(hover, alt.value(0.6), alt.value(0))
    ).transform_filter(hover)
    return style_chart((line + rule + points).properties(height=280))


def category_chart(df):
    df = df.assign(revenue=df["revenue"].astype(float))
    chart = (
        alt.Chart(df)
        .mark_bar(color=SERIES_COLOR, cornerRadiusEnd=2, height={"band": 0.8})
        .encode(
            y=alt.Y("category:N", sort="-x", title=None),
            x=alt.X("revenue:Q", title=f"CA ({CURRENCY})", axis=alt.Axis(format="~s")),
            tooltip=[
                alt.Tooltip("category:N", title="Catégorie"),
                alt.Tooltip("revenue:Q", title=f"CA ({CURRENCY})", format=",.2f"),
                alt.Tooltip("quantity:Q", title="Articles vendus"),
            ],
        )
        .properties(height=max(240, 22 * len(df)))
    )
    return style_chart(chart)


def _land_rings():
    """Contours des États-Unis (liste d'anneaux lon/lat), décodés du TopoJSON local."""
    scale, translate = US_STATES["transform"]["scale"], US_STATES["transform"]["translate"]
    arcs = []
    for arc in US_STATES["arcs"]:
        points = np.cumsum(np.array(arc, dtype=float), axis=0)
        arcs.append(points * scale + translate)

    def ring(indexes):
        parts = [arcs[i] if i >= 0 else arcs[~i][::-1] for i in indexes]
        return np.vstack([parts[0]] + [part[1:] for part in parts[1:]])

    geometry = US_STATES["objects"]["nation"]["geometries"][0]
    polygons = geometry["arcs"] if geometry["type"] == "MultiPolygon" else [geometry["arcs"]]
    return [ring(r) for polygon in polygons for r in polygon]


def _inside(rings, lon, lat):
    """Règle pair/impair (lancer de rayon) : vrai si le point est sur le territoire."""
    inside = np.zeros(lon.shape, dtype=bool)
    for r in rings:
        x1, y1 = r[:-1, 0], r[:-1, 1]
        x2, y2 = r[1:, 0], r[1:, 1]
        for xa, ya, xb, yb in zip(x1, y1, x2, y2):
            crosses = (ya > lat) != (yb > lat)
            if not crosses.any():
                continue
            x_at = xa + (lat[crosses] - ya) * (xb - xa) / (yb - ya)
            inside[crosses] ^= lon[crosses] < x_at
    return inside


@st.cache_data
def land_grid():
    """Grille régulière couvrant les 48 États contigus, limitée à la terre ferme."""
    lons, lats = np.meshgrid(np.arange(-125, -66.5, HEAT_STEP), np.arange(24.5, 49.5, HEAT_STEP))
    lon, lat = lons.ravel(), lats.ravel()
    mask = _inside(_land_rings(), lon, lat)
    return lon[mask], lat[mask]


def heat_grid(df, column):
    """Intensité de chaque case de la grille : noyau gaussien centré sur chaque ville.

    Volumes (CA, commandes) : somme des noyaux pondérés, ramenée à 0-100 (100 = zone la plus dense).
    Taux : moyenne des taux des villes pondérée par la proximité (une moyenne, pas une somme).
    """
    lon, lat = land_grid()
    dx = (lon[:, None] - df["lon"].to_numpy()[None, :]) * np.cos(np.radians(lat))[:, None]
    dy = lat[:, None] - df["lat"].to_numpy()[None, :]
    kernel = np.exp(-(dx**2 + dy**2) / (2 * HEAT_SIGMA**2))
    values = df[column].to_numpy(dtype=float)
    coverage = kernel.sum(axis=1)
    if column == "cancel_rate":
        value = (kernel * values[None, :]).sum(axis=1) / np.where(coverage > 0, coverage, 1)
        keep = coverage >= HEAT_MIN_SHARE * max(coverage.max(), 1e-9)
    else:
        weighted = (kernel * values[None, :]).sum(axis=1)
        value = weighted / (weighted.max() or 1.0) * 100
        keep = value >= HEAT_MIN_SHARE * 100
    return pd.DataFrame({"lon": lon[keep], "lat": lat[keep], "value": value[keep]})


def map_chart(df, metric):
    column = MAP_METRICS[metric]
    grid = heat_grid(df, column)
    is_rate = column == "cancel_rate"
    domain = [float(grid["value"].min()), float(grid["value"].max())] if is_rate else [0, 100]
    if domain[0] == domain[1]:
        domain = [domain[0] - 1, domain[1] + 1]
    states = (
        alt.Chart(alt.Data(values=US_STATES, format=alt.DataFormat(type="topojson", feature="states")))
        .mark_geoshape(fill="#F4F6F9", stroke="#FFFFFF", strokeWidth=0.8)
    )
    heat = (
        alt.Chart(grid)
        .mark_circle(size=70, opacity=0.9)
        .encode(
            longitude="lon:Q", latitude="lat:Q",
            color=alt.Color(
                "value:Q",
                scale=alt.Scale(domain=domain, range=HEAT_COLORS, interpolate="rgb"),
                legend=alt.Legend(
                    title="Taux d'annulation (%)" if is_rate else "Intensité relative",
                    orient="bottom", direction="horizontal", gradientLength=260, format=".1f" if is_rate else "d",
                ),
            ),
            tooltip=alt.value(None),
        )
    )
    cities = (
        alt.Chart(df)
        .mark_circle(color="#18212C", size=22, opacity=0.8, stroke="#FFFFFF", strokeWidth=0.8)
        .encode(
            longitude="lon:Q", latitude="lat:Q",
            tooltip=[
                alt.Tooltip("city:N", title="Ville"),
                alt.Tooltip("revenue:Q", title=f"CA ({CURRENCY})", format=",.2f"),
                alt.Tooltip("orders:Q", title="Commandes", format=",d"),
                alt.Tooltip("cancelled:Q", title="Annulées", format=",d"),
                alt.Tooltip("cancel_rate:Q", title="Taux d'annulation (%)", format=".1f"),
            ],
        )
    )
    labels = (
        alt.Chart(df.nlargest(8, column))
        .mark_text(dy=-8, fontSize=11, color="#18212C", fontWeight=600)
        .encode(longitude="lon:Q", latitude="lat:Q", text="city:N")
    )
    chart = alt.layer(states, heat, cities, labels).project("albersUsa").properties(height=520)
    return style_chart(chart)


def style_chart(chart):
    return (
        chart.configure(background=CHART_BACKGROUND, padding={"left": 12, "right": 12, "top": 4, "bottom": 4}, autosize={"type": "fit", "contains": "padding"})
        .configure_view(stroke=None)
        .configure_axis(gridColor=LINE_COLOR, domainColor=LINE_COLOR, tickColor=LINE_COLOR,
                        labelColor=MUTED_COLOR, titleColor=MUTED_COLOR)
    )


def status_style(label):
    return STATUS_STYLES.get(label, "")


def fetch():
    """Charge les données et affiche la ligne de fraîcheur ; None si rien à afficher."""
    now = datetime.now(timezone.utc)
    try:
        data = load()
    except (psycopg2.Error, KeyError) as exc:
        st.error(f"Connexion à PostgreSQL impossible : {exc}. Nouvel essai dans {REFRESH_SECONDS} s.")
        return None

    kpis = data["kpis"].iloc[0]
    text, level = freshness(kpis["last_aggregate"], now)
    status_line = f"Dernière actualisation : {now:%H:%M:%S} UTC — {text}"
    (st.success if level == "success" else st.warning)(status_line)

    if int(kpis["orders"]) == 0 and data["categories"].empty:
        st.info("Aucune donnée pour l'instant : lancer le simulateur (`docker compose start simulator`).")
        return None
    return data


@st.fragment(run_every=REFRESH_SECONDS)
def overview_page():
    """Page 1 : indicateurs clés et graphiques agrégés uniquement."""
    data = fetch()
    if data is None:
        return
    kpis = data["kpis"].iloc[0]

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric(f"Chiffre d'affaires ({CURRENCY})", money(float(kpis["revenue"])))
    c2.metric("Commandes", number(kpis["orders"]))
    c3.metric("Commandes payées", number(kpis["paid_orders"]),
              help="Commandes dont le paiement a réussi (statut PAID).")
    c4.metric("Articles vendus", number(kpis["quantity"]))
    c5.metric("Catégories représentées", number(kpis["categories"]))

    st.subheader("Évolution du chiffre d'affaires")
    choice, switch = st.columns([4, 1], vertical_alignment="center")
    period = choice.radio("Période", list(TIMELINE_PERIODS), horizontal=True, key="timeline_period",
                          label_visibility="collapsed")
    cumulative = switch.toggle("Cumulé", key="timeline_cumulative",
                               help="Additionne le CA tranche après tranche : la courbe ne fait que croître.")
    timeline = prepare_timeline(data["timeline"], period, cumulative)
    if timeline.empty:
        st.info(f"Pas de fenêtre sur la période « {period.lower()} ».")
    else:
        st.altair_chart(timeline_chart(timeline, period, cumulative), use_container_width=True)

    st.subheader("Répartition du CA par catégorie")
    st.altair_chart(category_chart(data["categories"]), use_container_width=True)


@st.fragment(run_every=REFRESH_SECONDS)
def orders_page():
    """Page 2 : suivi détaillé des commandes et des agrégations."""
    data = fetch()
    if data is None:
        return

    st.subheader(f"Suivi des commandes ({ORDERS_LIMIT} dernières)")
    orders = data["latest_orders"].copy()
    orders["status"] = orders["status"].map(STATUS_LABELS).fillna(orders["status"])
    st.dataframe(
        orders.style.map(status_style, subset=["status"]),
        hide_index=True, use_container_width=True,
        column_config={
            "order_id": "Commande",
            "created_at": st.column_config.DatetimeColumn("Créée le (UTC)", format="HH:mm:ss"),
            "customer": "Client",
            "city": "Ville",
            "status": "Statut",
            "total_amount": st.column_config.NumberColumn(f"Montant ({CURRENCY})", format="%.2f"),
        },
    )

    left, right = st.columns(2)
    with left:
        st.subheader("Top 10 des catégories")
        top = data["categories"].head(10).copy()
        total = float(data["categories"]["revenue"].sum()) or 1.0
        top["part"] = top["revenue"].astype(float) / total * 100
        st.dataframe(
            top[["category", "revenue", "part", "orders", "quantity"]],
            hide_index=True, use_container_width=True,
            column_config={
                "category": "Catégorie",
                "revenue": st.column_config.NumberColumn(f"CA ({CURRENCY})", format="%.2f"),
                "part": st.column_config.ProgressColumn("Part du CA", format="%.1f %%",
                                                        min_value=0, max_value=100),
                "orders": "Commandes",
                "quantity": "Articles",
            },
        )
    with right:
        st.subheader("Dernières agrégations")
        st.dataframe(
            data["latest_aggregates"], hide_index=True, use_container_width=True,
            column_config={
                "window_start": st.column_config.DatetimeColumn("Début", format="HH:mm"),
                "window_end": st.column_config.DatetimeColumn("Fin", format="HH:mm"),
                "category": "Catégorie",
                "order_count": "Commandes",
                "total_quantity": "Articles",
                "total_revenue": st.column_config.NumberColumn(f"CA ({CURRENCY})", format="%.2f"),
                "updated_at": st.column_config.DatetimeColumn("Mis à jour (UTC)", format="HH:mm:ss"),
            },
        )


@st.fragment(run_every=REFRESH_SECONDS)
def map_page():
    """Page 3 : carte de chaleur des ventes aux États-Unis."""
    data = fetch()
    if data is None:
        return

    cities = data["cities"].copy()
    coords = pd.DataFrame(
        [(city, lat, lon) for city, (lat, lon) in CITY_COORDS.items()], columns=["city", "lat", "lon"]
    )
    cities = cities.merge(coords, on="city", how="left")
    cities["cancel_rate"] = cities["cancelled"] / cities["orders"].where(cities["orders"] > 0) * 100
    cities["cancel_rate"] = cities["cancel_rate"].fillna(0.0)
    mapped = cities.dropna(subset=["lat", "lon"])
    if mapped.empty:
        st.info(f"Aucune commande localisable sur les {HEATMAP_HOURS} dernières heures.")
        return

    top = cities.iloc[0]
    total_revenue = float(cities["revenue"].sum()) or 1.0
    worst = cities.loc[cities["cancel_rate"].idxmax()]
    c1, c2, c3 = st.columns(3)
    c1.metric(f"Villes actives ({HEATMAP_HOURS} h)", number(len(cities)),
              delta=f"{number(cities['orders'].sum())} commandes", delta_color="off")
    c2.metric(f"Première ville (CA, {HEATMAP_HOURS} h)", top["city"],
              delta=f"{float(top['revenue']) / total_revenue * 100:.1f} % du CA", delta_color="off")
    c3.metric(f"Taux d'annulation le plus élevé ({HEATMAP_HOURS} h)", f"{worst['cancel_rate']:.1f} %",
              delta=worst["city"], delta_color="off",
              help="Part des commandes de la ville au statut CANCELLED.")

    metric = st.radio("Indicateur affiché sur la carte", list(MAP_METRICS), horizontal=True)
    st.subheader(f"Carte de chaleur : {metric.lower()} sur les {HEATMAP_HOURS} dernières heures")
    st.caption(f"Commandes créées durant les {HEATMAP_HOURS} dernières heures. Position = ville du client (le pipeline ne transporte pas l'adresse de livraison complète). "
               "Plus la teinte est foncée, plus la valeur est élevée autour de la ville.")
    st.altair_chart(map_chart(mapped, metric), use_container_width=True)
    if len(mapped) < len(cities):
        missing = ", ".join(sorted(set(cities["city"]) - set(mapped["city"])))
        st.caption(f"Villes absentes de la carte (coordonnées inconnues) : {missing}.")

    st.subheader(f"Détail par ville ({HEATMAP_HOURS} dernières heures)")
    st.dataframe(
        cities[["city", "revenue", "orders", "paid", "cancelled", "cancel_rate"]],
        hide_index=True, use_container_width=True,
        column_config={
            "city": "Ville",
            "revenue": st.column_config.NumberColumn(f"CA ({CURRENCY})", format="%.2f"),
            "orders": "Commandes",
            "paid": "Payées",
            "cancelled": "Annulées",
            "cancel_rate": st.column_config.NumberColumn("Taux d'annulation", format="%.1f %%"),
        },
    )


st.set_page_config(page_title="RetailPulse", page_icon="📈", layout="wide")
st.markdown(
    f"""
    <style>
    [data-testid="stMetric"], [data-testid="stArrowVegaLiteChart"], [data-testid="stVegaLiteChart"] {{
        background: #FFFFFF; border: 1px solid {LINE_COLOR}; border-radius: {RADIUS}; padding: 12px 16px;
    }}
    /* Vega dimensionne le canvas sur toute la largeur de la carte : pas de marge horizontale, sinon il déborde. */
    [data-testid="stArrowVegaLiteChart"], [data-testid="stVegaLiteChart"] {{ padding-left: 0; padding-right: 0; }}
    [data-testid="stDataFrame"] {{
        border: 1px solid {LINE_COLOR}; border-radius: {RADIUS}; overflow: hidden;
    }}
    [data-testid="stAlert"] {{ border-radius: {RADIUS}; }}
    [data-testid="stMetricLabel"] {{ color: {MUTED_COLOR}; }}
    [data-testid="stMetricDelta"] svg {{ display: none; }}
    /* Lien Spark épinglé en bas de la barre latérale (le contenu de la page reste en haut). */
    [data-testid="stSidebarContent"] {{ display: flex; flex-direction: column; }}
    [data-testid="stSidebarContent"] > * {{ flex-shrink: 0; }}
    [data-testid="stSidebarUserContent"] {{ margin-top: auto; padding-top: 0; padding-bottom: 1rem; }}
    .spark-link {{
        display: flex; flex-direction: column; align-items: center; gap: 3px; padding: 6px 10px; margin: 0 4px;
        border: 1px solid {LINE_COLOR}; border-radius: {RADIUS}; background: #FFFFFF;
        color: {MUTED_COLOR} !important; text-decoration: none !important; font-size: 0.68rem;
    }}
    .spark-link:hover {{ border-color: #E25A1C; color: #18212C !important; }}
    .spark-link img {{ width: 84px; max-width: 100%; }}
    h1 {{ font-weight: 600; letter-spacing: -0.01em; }}
    h3 {{ font-weight: 600; }}
    </style>
    """,
    unsafe_allow_html=True,
)
st.title("RetailPulse — Performances commericales")
st.caption(
    f"Chiffre d'affaires = paiements réussis uniquement (PAYMENT_COMPLETED), montants bruts en {CURRENCY}. "
    f"Données lues dans PostgreSQL, calculées par Spark sur des fenêtres de 5 minutes, "
    f"recalculées toutes les 5 minutes. Page relue toutes les {REFRESH_SECONDS} s."
)

navigation = st.navigation(
    [
        st.Page(overview_page, title="Indicateurs", default=True),
        st.Page(orders_page, title="Suivi des commandes", url_path="commandes"),
        st.Page(map_page, title="Carte de chaleur", url_path="carte"),
    ]
)

spark_logo = base64.b64encode((Path(__file__).parent / "spark-logo.png").read_bytes()).decode()
st.sidebar.markdown(
    f"""
    <a class="spark-link" href="{SPARK_UI_URL}" target="_blank" rel="noopener noreferrer"
       title="Ouvrir l'interface Spark ({SPARK_UI_URL}) dans un nouvel onglet">
        <img src="data:image/png;base64,{spark_logo}" alt="Apache Spark">
        <span>Ouvrir l'interface Spark ↗</span>
    </a>
    """,
    unsafe_allow_html=True,
)
navigation.run()
