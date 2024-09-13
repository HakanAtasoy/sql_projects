select 
	name,
	fraud_count
from
	(select 
		pc.name,
		count(*) as fraud_count,
		ntile(4) over(order by count(*) desc ) as quartile
	from
		refunds r, orders o, products p, product_category pc, shopping_carts sc 
	where 
		r.order_id = o.order_id 
		and o.order_id  = sc.order_id
		and sc.product_id = p.product_id
		and p.category_id = pc.category_id
		and r.reason = 'FRAUD_SUSPICION'
	group by 
		pc.name) as fraud_quartile
where 
	quartile = 1
order by
	fraud_count desc;
