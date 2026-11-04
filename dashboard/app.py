"""Tableau de bord RetailPulse (section 10 du cahier des charges).

Lit uniquement les données persistées dans PostgreSQL (sales_aggregates, orders) :
aucune agrégation métier n'est recalculée ici, à une exception près : la page « Carte
de chaleur » regroupe les commandes par ville, car sales_aggregates ne porte pas la ville,
et la page « Tableau libre » regroupe à la demande les lignes lues (sommes et ratios de sommes).
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

# Tableau libre : périodes, granularités (pas, format d'affichage) et sources.
FREE_PERIODS = {
    "Tout l'historique": None,
    "6 dernières heures": 6,
    "Dernier jour": 24,
    "3 derniers jours": 72,
    "Dernière semaine": 168,
}
FREE_GRANULARITIES = {
    "5 minutes": ("5min", "DD/MM HH:mm"),
    "Heure": ("1h", "DD/MM HH:mm"),
    "Jour": ("1D", "DD/MM/YYYY"),
}
# kind : money / int / pct / ratio (formats d'affichage). Les mesures « dérivées » sont recalculées
# à partir des sommes du groupe (jamais par moyenne de moyennes).
FREE_SOURCES = {
    "Ventes par catégorie (agrégats Spark)": {
        "id": "sales",
        "query": """
            SELECT category, window_start AS period_ts, order_count AS orders,
                   total_quantity AS quantity, total_revenue AS revenue
            FROM sales_aggregates
            WHERE (%(hours)s::int IS NULL OR window_start >= now() - make_interval(hours => %(hours)s::int))
        """,
        "base": ["revenue", "orders", "quantity"],
        "dimensions": {"Catégorie": "category"},
        "measures": {
            f"Chiffre d'affaires ({CURRENCY})": ("revenue", "money"),
            "Commandes": ("orders", "int"),
            "Articles vendus": ("quantity", "int"),
            f"Panier moyen ({CURRENCY})": ("avg_basket", "money"),
            "Articles par commande": ("items_per_order", "ratio"),
        },
        "default_dimensions": ["Catégorie"],
        "default_measures": [f"Chiffre d'affaires ({CURRENCY})", "Commandes", "Articles vendus"],
    },
    "Commandes (PostgreSQL)": {
        "id": "orders",
        "query": """
            SELECT o.created_at AS period_ts, c.city, c.first_name || ' ' || c.last_name AS customer,
                   o.status, o.total_amount
            FROM orders o JOIN customers c USING (customer_id)
            WHERE (%(hours)s::int IS NULL OR o.created_at >= now() - make_interval(hours => %(hours)s::int))
        """,
        "base": ["orders", "paid", "cancelled", "refused", "revenue"],
        "dimensions": {"Ville": "city", "Statut": "status_label", "Client": "customer"},
        "measures": {
            "Commandes": ("orders", "int"),
            "Commandes payées": ("paid", "int"),
            "Paiements refusés": ("refused", "int"),
            "Commandes annulées": ("cancelled", "int"),
            f"Chiffre d'affaires ({CURRENCY})": ("revenue", "money"),
            f"Panier moyen des payées ({CURRENCY})": ("avg_basket", "money"),
            "Taux de paiement refusé": ("refusal_rate", "pct"),
            "Taux d'annulation": ("cancel_rate", "pct"),
        },
        "default_dimensions": ["Ville"],
        "default_measures": ["Commandes", "Commandes payées", f"Chiffre d'affaires ({CURRENCY})", "Taux d'annulation"],
    },
}

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


def connect():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"],
        connect_timeout=5,
    )


def load():
    connection = connect()
    try:
        with connection.cursor() as cursor:
            return {name: query(cursor, sql) for name, sql in QUERIES.items()}
    finally:
        connection.close()


def query(cursor, sql, params=None):
    cursor.execute(sql, params)
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


def load_free(source_id, hours):
    source = next(s for s in FREE_SOURCES.values() if s["id"] == source_id)
    connection = connect()
    try:
        with connection.cursor() as cursor:
            return query(cursor, source["query"], {"hours": hours})
    finally:
        connection.close()


def prepare_free(source_id, raw):
    df = raw.copy()
    if source_id == "sales":
        df[["revenue", "orders", "quantity"]] = df[["revenue", "orders", "quantity"]].astype(float)
        return df
    paid = df["status"] == "PAID"
    df["orders"] = 1
    df["paid"] = paid.astype(int)
    df["cancelled"] = (df["status"] == "CANCELLED").astype(int)
    df["refused"] = (df["status"] == "FAILED").astype(int)
    df["revenue"] = np.where(paid, df["total_amount"].astype(float), 0.0)
    df["status_label"] = df["status"].map(STATUS_LABELS).fillna(df["status"])
    return df


def derive_free(df):
    """Ajoute les ratios, calculés à partir des sommes déjà agrégées du groupe."""
    def ratio(numerator, denominator):
        return numerator / denominator.where(denominator != 0)

    df = df.copy()
    df["avg_basket"] = ratio(df["revenue"], df["paid"] if "paid" in df else df["orders"])
    if "quantity" in df:
        df["items_per_order"] = ratio(df["quantity"], df["orders"])
    if "refused" in df:
        df["refusal_rate"] = ratio(df["refused"], df["orders"]) * 100
        df["cancel_rate"] = ratio(df["cancelled"], df["orders"]) * 100
    return df


def measure_config(kind, label):
    formats = {"money": "%.2f", "int": "%d", "pct": "%.1f %%", "ratio": "%.1f"}
    return st.column_config.NumberColumn(label, format=formats[kind])


@st.fragment(run_every=REFRESH_SECONDS)
def free_table_page():
    """Page 4 : tableau libre, avec choix des champs, du niveau d'agrégation et des mesures."""
    if fetch() is None:
        return

    source_label = st.radio("Source de données", list(FREE_SOURCES), horizontal=True, key="free_source")
    source = FREE_SOURCES[source_label]
    sid = source["id"]
    dimension_options = ["Période", *source["dimensions"]]

    col_period, col_dims, col_measures = st.columns([1, 1.3, 2])
    period = col_period.selectbox("Période analysée", list(FREE_PERIODS), index=2, key=f"free_period_{sid}")
    dimensions = col_dims.multiselect("Regrouper par", dimension_options,
                                      default=source["default_dimensions"], key=f"free_dims_{sid}")
    measures = col_measures.multiselect("Mesures", list(source["measures"]),
                                        default=source["default_measures"], key=f"free_measures_{sid}")
    if not measures:
        st.info("Choisir au moins une mesure pour générer le tableau.")
        return
    has_period = "Période" in dimensions

    sort_options = [*dimensions, *measures]
    col_gran, col_sort, col_order, col_rows = st.columns([1, 1.3, 1, 1])
    granularity = col_gran.selectbox("Granularité de la période", list(FREE_GRANULARITIES), index=1,
                                     disabled=not has_period, key=f"free_gran_{sid}",
                                     help="Active seulement si « Période » fait partie des champs de regroupement.")
    sort_default = sort_options.index("Période") if has_period else len(dimensions)
    sort_by = col_sort.selectbox("Trier par", sort_options, index=sort_default)
    descending = col_order.radio("Ordre", ["Décroissant", "Croissant"], index=1 if has_period else 0,
                                 horizontal=True) == "Décroissant"
    limit = col_rows.number_input("Lignes affichées (max)", min_value=5, max_value=1000, value=50, step=5,
                                  key=f"free_limit_{sid}")

    try:
        raw = load_free(sid, FREE_PERIODS[period])
    except psycopg2.Error as exc:
        st.error(f"Lecture impossible : {exc}")
        return
    if raw.empty:
        st.info(f"Aucune donnée sur la période « {period.lower()} ».")
        return

    freq, date_format = FREE_GRANULARITIES[granularity]
    df = prepare_free(sid, raw)
    columns = {label: source["dimensions"][label] for label in dimensions if label != "Période"}
    if has_period:
        df["period"] = df["period_ts"].dt.floor(freq)
        columns = {"Période": "period", **columns}
    base = source["base"]
    if columns:
        grouped = df.groupby(list(columns.values()), dropna=False)[base].sum().reset_index()
    else:
        grouped = df[base].sum().to_frame().T
    grouped = derive_free(grouped)
    totals = derive_free(df[base].sum().to_frame().T)

    # Libellés d'affichage, dans l'ordre choisi par l'utilisateur.
    measure_columns = {label: source["measures"][label][0] for label in measures}
    kinds = {label: source["measures"][label][1] for label in measures}
    table = grouped[[*columns.values(), *measure_columns.values()]]
    table.columns = [*columns.keys(), *measure_columns.keys()]
    table = table.sort_values(sort_by, ascending=not descending, na_position="last")

    shown = table.head(int(limit))
    config = {label: measure_config(kinds[label], label) for label in measures}
    if has_period:
        config["Période"] = st.column_config.DatetimeColumn("Période", format=date_format)
    styled = shown.style.map(status_style, subset=["Statut"]) if "Statut" in shown else shown
    st.caption(f"{len(shown)} ligne(s) affichée(s) sur {len(table)} — {len(raw)} enregistrement(s) lus "
               f"sur la période « {period.lower()} ».")
    if sid == "sales":
        st.caption("Les commandes sont comptées par catégorie : une commande de plusieurs catégories compte dans chacune.")
    st.dataframe(styled, hide_index=True, use_container_width=True, column_config=config)

    total_table = totals[list(measure_columns.values())]
    total_table.columns = list(measure_columns.keys())
    st.markdown("**Total de la sélection**")
    st.dataframe(total_table, hide_index=True, use_container_width=True,
                 column_config={label: measure_config(kinds[label], label) for label in measures})

    export = table.copy()
    if "Période" in export:
        export["Période"] = export["Période"].dt.strftime("%Y-%m-%d %H:%M")
    st.download_button("Exporter en CSV", export.to_csv(index=False, sep=";", decimal=",").encode("utf-8-sig"),
                       file_name="tableau_libre.csv", mime="text/csv")


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
        st.Page(free_table_page, title="Tableau libre", url_path="tableau-libre"),
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
