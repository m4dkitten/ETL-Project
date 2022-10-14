SELECT
    c.id_customer,
    c.birthdate_customer,
    date_part('year', age(TO_DATE(c.birthdate_customer, 'YYYY-MM-DD'))) AS age,
    c.country_customer,
    c.gender_customer,
    p."Type",
    t.id_transaction,
    t.date_transaction,
    t.product_transaction,
    t.amount_transaction
FROM bigdata_customer AS c
    LEFT JOIN bigdata_transaction AS t ON c.id_customer = t.id_customer
    LEFT JOIN bigdata_product AS p ON p."Type" = t.product_transaction