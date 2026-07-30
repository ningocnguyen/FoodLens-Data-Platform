TOP_BRANDS = """
SELECT *
FROM brand_summary
ORDER BY product_count DESC
LIMIT 20;
"""