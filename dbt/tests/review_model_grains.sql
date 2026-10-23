SELECT review_id, order_id
FROM {{ ref('fct_reviews') }}
GROUP BY review_id, order_id HAVING COUNT(*) > 1
