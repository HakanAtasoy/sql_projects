select 
	name,
	product_id,
	total_ordered,
    total_ordered - lag(total_ordered) over (order by total_ordered desc) as diff_with_previous
from
	(select 
	    pc.name,
	    p.product_id,
	    sum(sc.amount) as total_ordered,
	    row_number() over (partition by pc.name order by sum(sc.amount) desc) as row_num
	from
		orders o, shopping_carts sc, products p, product_category pc 
	where
		o.order_id = sc.order_id
		and sc.product_id = p.product_id
		and p.category_id = pc.category_id
	group by 
		pc.name, p.product_id) as subquery
where 
	row_num = 1;