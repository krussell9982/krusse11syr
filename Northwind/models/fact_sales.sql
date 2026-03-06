
with stg_orders as (
    select
        OrderID,
        {{ dbt_utils.generate_surrogate_key(['employeeid']) }} as employeekey,
        {{ dbt_utils.generate_surrogate_key(['customerid']) }} as customerkey,
        -- Converts 2023-01-01 to 20230101
        replace(to_date(orderdate)::varchar, '-', '')::int as orderdatekey
    from {{ source('northwind', 'Orders') }}
),

stg_order_details as (
    select
        orderid,
        productid,
        sum(Quantity) as quantityonorder,
        sum(Quantity * UnitPrice) as extendedpriceamount,
        -- Calculated directly to avoid alias reference errors
        sum((Quantity * UnitPrice) * Discount) as discountamount,
        sum((Quantity * UnitPrice) * (1 - Discount)) as soldamount
    from {{ source('northwind', 'Order_Details') }}
    group by 1, 2
)

select
    o.OrderID,
    o.employeekey,
    o.customerkey,
    o.orderdatekey,
    od.productid,
    od.quantityonorder,
    od.extendedpriceamount,
    od.discountamount,
    od.soldamount
from stg_orders o
inner join stg_order_details od 
    on o.orderid = od.orderid