"""Tableau de bord RetailPulse (section 10 du cahier des charges).

Lit uniquement les données persistées dans PostgreSQL (sales_aggregates, orders) :
aucune agrégation métier n'est recalculée ici. Actualisation automatique.
"""
import os
from datetime import datetime, timezone
from decimal import Decimal

import altair as alt
import pandas as pd
import psycopg2
import streamlit as st

REFRESH_SECONDS = int(os.getenv("REFRESH_SECONDS", "10"))
STALE_AFTER_SECONDS = int(os.getenv("STALE_AFTER_SECONDS", "60"))
CURRENCY = "USD"
SERIES_COLOR = "#1F5FBF"  # une seule série par graphique : une seule teinte (thème Ardoise)
MUTED_COLOR = "#5B6672"
LINE_COLOR = "#CFD6DF"
RADIUS = "2px"

STATUS_LABELS = {
    "PAID": "✅ Payée",
    "FAILED": "❌ Paiement échoué",
    "CANCELLED": "⛔ Annulée",
    "CREATED": "⏳ En attente de paiement",
}

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
        WHERE window_start >= now() - interval '6 hours'
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
    "latest_orders": """
        SELECT o.order_id, o.created_at, c.first_name || ' ' || c.last_name AS customer,
               c.city, o.status, o.total_amount
        FROM orders o JOIN customers c USING (customer_id)
        ORDER BY o.created_at DESC LIMIT 15
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


def timeline_chart(df):
    df = df.assign(revenue=df["revenue"].astype(float))
    hover = alt.selection_point(fields=["window_start"], nearest=True, on="pointerover", empty=False)
    base = alt.Chart(df).encode(
        x=alt.X("window_start:T", title=None, axis=alt.Axis(format="%H:%M", grid=False)),
        y=alt.Y("revenue:Q", title=f"CA par fenêtre ({CURRENCY})", axis=alt.Axis(format="~s")),
    )
    line = base.mark_line(color=SERIES_COLOR, strokeWidth=2)
    points = base.mark_point(color=SERIES_COLOR, filled=True, size=70).encode(
        opacity=alt.condition(hover, alt.value(1), alt.value(0)),
        tooltip=[
            alt.Tooltip("window_start:T", title="Fenêtre", format="%H:%M"),
            alt.Tooltip("revenue:Q", title=f"CA ({CURRENCY})", format=",.2f"),
            alt.Tooltip("orders:Q", title="Commandes (somme par catégorie)"),
        ],
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


def style_chart(chart):
    return (
        chart.configure_view(stroke=None)
        .configure_axis(gridColor=LINE_COLOR, domainColor=LINE_COLOR, tickColor=LINE_COLOR,
                        labelColor=MUTED_COLOR, titleColor=MUTED_COLOR)
    )


st.set_page_config(page_title="RetailPulse", page_icon="📈", layout="wide")
st.markdown(
    f"""
    <style>
    [data-testid="stMetric"], [data-testid="stArrowVegaLiteChart"], [data-testid="stVegaLiteChart"] {{
        background: #FFFFFF; border: 1px solid {LINE_COLOR}; border-radius: {RADIUS}; padding: 12px 16px;
    }}
    [data-testid="stDataFrame"] {{
        border: 1px solid {LINE_COLOR}; border-radius: {RADIUS}; overflow: hidden;
    }}
    [data-testid="stAlert"] {{ border-radius: {RADIUS}; }}
    [data-testid="stMetricLabel"] {{ color: {MUTED_COLOR}; }}
    h1 {{ font-weight: 600; letter-spacing: -0.01em; }}
    h3 {{ font-weight: 600; }}
    </style>
    """,
    unsafe_allow_html=True,
)
st.title("RetailPulse — activité e-commerce en quasi-temps réel")
st.caption(
    f"Chiffre d'affaires = paiements réussis uniquement (PAYMENT_COMPLETED), montants bruts en {CURRENCY}. "
    f"Données lues dans PostgreSQL, calculées par Spark sur des fenêtres de 5 minutes. "
    f"Actualisation toutes les {REFRESH_SECONDS} s."
)


@st.fragment(run_every=REFRESH_SECONDS)
def render():
    now = datetime.now(timezone.utc)
    try:
        data = load()
    except (psycopg2.Error, KeyError) as exc:
        st.error(f"Connexion à PostgreSQL impossible : {exc}. Nouvel essai dans {REFRESH_SECONDS} s.")
        return

    kpis = data["kpis"].iloc[0]
    text, level = freshness(kpis["last_aggregate"], now)
    status_line = f"Dernière actualisation : {now:%H:%M:%S} UTC — {text}"
    (st.success if level == "success" else st.warning)(status_line)

    if int(kpis["orders"]) == 0 and data["categories"].empty:
        st.info("Aucune donnée pour l'instant : lancer le simulateur (`docker compose start simulator`).")
        return

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric(f"Chiffre d'affaires ({CURRENCY})", money(float(kpis["revenue"])))
    c2.metric("Commandes", number(kpis["orders"]))
    c3.metric("Commandes payées", number(kpis["paid_orders"]),
              help="Commandes dont le paiement a réussi (statut PAID).")
    c4.metric("Articles vendus", number(kpis["quantity"]))
    c5.metric("Catégories représentées", number(kpis["categories"]))

    st.subheader("Évolution du chiffre d'affaires (fenêtres de 5 minutes, 6 dernières heures)")
    if data["timeline"].empty:
        st.info("Pas de fenêtre sur les 6 dernières heures.")
    else:
        st.altair_chart(timeline_chart(data["timeline"]), use_container_width=True)

    left, right = st.columns([3, 2])
    with left:
        st.subheader("Répartition du CA par catégorie")
        st.altair_chart(category_chart(data["categories"]), use_container_width=True)
    with right:
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

    left, right = st.columns(2)
    with left:
        st.subheader("Dernières commandes")
        orders = data["latest_orders"].copy()
        orders["status"] = orders["status"].map(STATUS_LABELS).fillna(orders["status"])
        st.dataframe(
            orders, hide_index=True, use_container_width=True,
            column_config={
                "order_id": "Commande",
                "created_at": st.column_config.DatetimeColumn("Créée le (UTC)", format="HH:mm:ss"),
                "customer": "Client",
                "city": "Ville",
                "status": "Statut",
                "total_amount": st.column_config.NumberColumn(f"Montant ({CURRENCY})", format="%.2f"),
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


render()
