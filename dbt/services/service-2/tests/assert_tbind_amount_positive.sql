SELECT * FROM {{ ref('tbind') }} WHERE amount_eur < 0
