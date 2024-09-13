select
    customer_id,
    order_year as year,
    order_month as month,
    max(cart_total) AS cart_total
from(
	select 
		sum(p.price * sc.amount) as cart_total,
		extract(month from o.order_time) as order_month,
		extract(year from o.order_time) as order_year,
        o.customer_id,
        row_number() over (partition by extract(month from o.order_time) order by sum(p.price * sc.amount) desc) as row_num
	from 
		orders o, shopping_carts sc, products p
	where 
		o.order_id  = sc.order_id 
		and sc.product_id = p.product_id 
	group by 
		o.order_id, o.customer_id, order_month, order_year
	) as subquery
where 
	row_num = 1
group by
    order_month, customer_id, order_year
order by order_month;