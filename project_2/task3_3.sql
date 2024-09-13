-- for the purpose of the example I chose the first customer id in the table
create view customer_shopping_info as 
select 
    c.customer_id as customer_id,
    c.name,
    c.surname,
    o.order_id,
    o.order_time,
    o.shipping_time,
    o.status,
    p.product_id,
    p.name as product_name
from 
	customers c, orders o, shopping_carts sc, products p
where  
	c.customer_id = o.customer_id
	and o.order_id = sc.order_id
	and sc.product_id = p.product_id
	and c.customer_id = 'd1e132f5-0ee9-488e-9eee-3b6f298ac2c5';   -- this would be set depending on which customer is viewing the data
	
	
-- testing the view
select 
	* 
from 
	customer_shopping_info 
	
	
-- and to make sure customers only have access to their view and not refunds etc
-- of course this is examplary since there is no customer role etc
revoke all on all tables in schema public from customers;
grant select 
on customer_shopping_info 
to customers



/*
 * reasoning for using standard view over materialized view:
 * Up to date data: customers need to see their shopping cart items in real time
 */
