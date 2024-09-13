create table category_avg_prices (
	category_id int primary key,
	avg_price decimal(26,13)
);


insert into 
	category_avg_prices (category_id, avg_price)
select 
	category_id,
	avg(price) as avg_price
 from 
 	products p
 group by
 	category_id
 order by 
 	category_id;

 	
 	
-- trigger to update avg prices table
create or replace function 
	update_avg_prices() 
returns trigger as $$
begin 
	update 
		category_avg_prices
	set 
		avg_price = (select avg(price) from products where category_id = new.category_id)
	where 
		category_id = new.category_id;
	return new;
end;
$$ language plpgsql;
 	
create trigger
	product_insert_trigger
after insert on 
	products
for each row 
	execute function update_avg_prices();

-- testing to see if the trigger works
INSERT INTO products (product_id, name, price, category_id) VALUES
('52dd097b-4999-4f9b-9886-91b77d2feecb', 'Product 1', 1500, 1),
('8fc05e89-3fe1-4260-b172-d02d7b4e7c4f', 'Product 2', 4545, 1),
('c64b6da8-5d84-46ec-ae5c-3f7f59e5cc6a', 'Product 3', 54351, 2);

--checking the effects
select 
	*
from 
	category_avg_prices
order by 
	category_id;

-- restoring the products table to its original state (but averages table needs to be created from scratch to get intial results)
delete 
from 
	products 
where 
	name like 'Product%';







	