select
	c.gender,
    to_char(order_time, 'Month') as month,
    sum(p.price * sc.amount) as cart_total
from
    orders o, customers c, shopping_carts sc, products p
where
	o.order_id = sc.order_id
	and sc.product_id = p.product_id
	and o.order_id = sc.order_id
	and c.customer_id = o.customer_id
group by
    cube(gender, to_char(order_time, 'Month'))
order by
    gender, month;
    
   
   
   
 -- crosstab part  
create extension if not exists tablefunc;

 
select *
from 
	crosstab('select
				c.gender,
			    to_char(order_time, ''Month'') as month,
			    sum(p.price * sc.amount) as cart_total
			from
			    orders o, customers c, shopping_carts sc, products p
			where
				o.order_id = sc.order_id
				and sc.product_id = p.product_id
				and o.order_id = sc.order_id
				and c.customer_id = o.customer_id
			group by
			    cube(gender, to_char(order_time, ''Month''))
			order by
			    gender, month;')
	as ("gender" varchar,  "January" numeric, "February" numeric, "March" numeric);
  
  
  
