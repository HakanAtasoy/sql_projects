select
    category,
    product_name,
    refunded_orders,
    product_rank
from
    (select
        pc.name as category,
        p.name as product_name,
        count(r.order_id) as refunded_orders,
        rank() over (partition by pc.name order by count(r.order_id) desc) as product_rank
    from
        products p, shopping_carts sc, refunds r, product_category pc
    where
        sc.order_id = r.order_id
        and sc.product_id = p.product_id
        and p.category_id = pc.category_id
        and r.reason = 'DAMAGED_DELIVERY'
    group by
        pc.name, p.name) as refundedorders
order by 
	category asc;