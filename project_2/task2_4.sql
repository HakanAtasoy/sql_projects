select 
	weekday,
	avg_daily_revenue
from
	(select 
		to_char(o.order_time, 'Day') as weekday,
		sum(p.price * sc.amount) / count (distinct cast(o.order_time as date)) as avg_daily_revenue
	from 
		orders o, shopping_carts sc, products p
	where 
		o.order_id = sc.order_id
		and sc.product_id = p.product_id
		and o.status != 'CANCELLED'
		and o.order_id not in (select distinct order_id from refunds)
	group by 
		to_char(o.order_time, 'Day'))
order by 
	avg_daily_revenue desc 
limit 3;

	
	