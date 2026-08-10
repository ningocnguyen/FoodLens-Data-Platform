import plotly.express as px
import streamlit as st
from athena_client import run_query

st.set_page_config(
    page_title="FoodLens Dashboard",
    page_icon="🥗",
    layout="wide"
)

st.title("🥗 FoodLens Dashboard")

st.markdown(
    "Interactive Dashboard with **Amazon Athena**."
)

# Brand analysis
brand_df = run_query("""
SELECT *
FROM brand_summary
ORDER BY product_count DESC
LIMIT 20
""")

brand_df["product_count"] = brand_df["product_count"].astype(int)

brand_fig = px.bar(
    brand_df,
    x="brand",
    y="product_count",
    title="Top Brands by Product Count",
)

st.plotly_chart(
    brand_fig,
    width="stretch",
    theme="streamlit",
)

st.divider()

# Category analysis
category_df = run_query("""
SELECT *
FROM category_summary
ORDER BY product_count DESC
LIMIT 20
""")

category_df["product_count"] = category_df["product_count"].astype(int)

category_fig = px.bar(
    category_df,
    x="category",
    y="product_count",
    title="Top Categories by Product Count",
)

st.plotly_chart(category_fig, width="stretch")

st.divider()

# Product explorer
products_df = run_query("""
SELECT
    product_name,
    brand,
    nutrition_grade,
    energy_kcal_100g,
    sugars_100g
FROM fact_products
LIMIT 500
""")

st.subheader("Product Explorer")

st.dataframe(
    products_df,
    use_container_width=True,
    hide_index=True,
)

grade_df = run_query("""
SELECT
    nutrition_grade,
    COUNT(*) AS product_count
FROM fact_products
WHERE nutrition_grade IS NOT NULL
  AND nutrition_grade <> ''
GROUP BY nutrition_grade
ORDER BY nutrition_grade
""")

grade_df["product_count"] = grade_df["product_count"].astype(int)

grade_fig = px.pie(
    grade_df,
    names="nutrition_grade",
    values="product_count",
    title="Nutrition Grade Distribution",
    hole=0.45,
)

st.plotly_chart(grade_fig, use_container_width=True)

grade_nutrition_df = run_query("""
SELECT
    nutrition_grade,
    AVG(sugars_100g) AS average_sugars_100g
FROM fact_products
WHERE nutrition_grade IS NOT NULL
  AND nutrition_grade <> ''
  AND sugars_100g IS NOT NULL
GROUP BY nutrition_grade
ORDER BY nutrition_grade
""")

grade_nutrition_df["average_sugars_100g"] = (
    grade_nutrition_df["average_sugars_100g"].astype(float)
)

sugar_fig = px.bar(
    grade_nutrition_df,
    x="nutrition_grade",
    y="average_sugars_100g",
    title="Average Sugar by Nutrition Grade",
    labels={
        "nutrition_grade": "Nutrition Grade",
        "average_sugars_100g": "Average Sugar per 100g",
    },
)

st.plotly_chart(sugar_fig, use_container_width=True)

scatter_df = run_query("""
SELECT
    product_name,
    brand,
    nutrition_grade,
    energy_kcal_100g,
    sugars_100g
FROM fact_products
WHERE energy_kcal_100g IS NOT NULL
  AND sugars_100g IS NOT NULL
LIMIT 1000
""")

scatter_df["energy_kcal_100g"] = (
    scatter_df["energy_kcal_100g"].astype(float)
)

scatter_df["sugars_100g"] = (
    scatter_df["sugars_100g"].astype(float)
)

scatter_fig = px.scatter(
    scatter_df,
    x="sugars_100g",
    y="energy_kcal_100g",
    color="nutrition_grade",
    hover_data=["product_name", "brand"],
    title="Sugar vs. Calories",
    labels={
        "sugars_100g": "Sugar per 100g",
        "energy_kcal_100g": "Calories per 100g",
    },
)

st.plotly_chart(scatter_fig, use_container_width=True)