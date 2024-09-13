-- create trigger function
create or replace function check_shopping_cart_amount()
returns trigger as $$
begin 
	if new.amount <= 0 then 
		raise exception 'Amount must be greater than 0 for shopping cart item';
	end if;
	return new;
end;
$$ language plpgsql;

-- create trigger using trigger function
create trigger shopping_cart_amount_check_trigger
before insert on shopping_carts
for each row 
execute function check_shopping_cart_amount();


-- check if the trigger works
insert into shopping_carts(order_id, product_id, amount) 
values('ab70e2d3-a683-46e9-9c35-64c2da65384f', '8d94cdc9-e74d-4cf6-9737-6bcb96b76912', 0);