select distinct 
    customer_id,
    gender,
    sum (refund_amount) over (partition by customer_id) 
from (
    select
        c.customer_id,
        c.gender,
        p.price * sc.amount as refund_amount
    from
        refunds r, shopping_carts sc,products p ,orders o ,customers c
    where
	    r.order_id = sc.order_id
	    and sc.product_id = p.product_id
	    and r.order_id = o.order_id
	    and o.customer_id = c.customer_id
	    and o.status = 'COMPLETED'
) as subquery
order by customer_id;