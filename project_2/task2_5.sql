select
    least(pc1.name, pc2.name) as category1,
    greatest(pc1.name, pc2.name) as category2,
    count(*) as total_count
from 
    orders o, shopping_carts sc1, shopping_carts sc2, products p1, products p2, product_category pc1, product_category pc2
where 
	o.order_id = sc1.order_id
	and o.order_id = sc2.order_id
	and sc1.product_id = p1.product_id  
	and sc2.product_id = p2.product_id
	and p1.category_id = pc1.category_id
	and p2.category_id = pc2.category_id 
	and o.status != 'CANCELLED'
	and o.order_id not in (select order_id from refunds r )
	and p1.category_id < p2.category_id -- Ensure pairs are distinct and ordered
group by
    pc1.name, pc2.name
order by
	total_count  desc 
limit 10;