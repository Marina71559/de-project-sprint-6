<!-- Список полей, которые необходимы для витрины. -->
Состав витрины:

    id — идентификатор записи.
    courier_id — ID курьера, которому перечисляем.
    courier_name — Ф. И. О. курьера.
    settlement_year — год отчёта.
    settlement_month — месяц отчёта, где 1 — январь и 12 — декабрь.
    orders_count — количество заказов за период (месяц).
    orders_total_sum — общая стоимость заказов.
    rate_avg — средний рейтинг курьера по оценкам пользователей.
    order_processing_fee — сумма, удержанная компанией за обработку заказов, которая высчитывается как orders_total_sum * 0.25.
    courier_order_sum — сумма, которую необходимо перечислить курьеру за доставленные им/ей заказы. За каждый доставленный заказ курьер должен получить некоторую сумму в зависимости от рейтинга (см. ниже).
    courier_tips_sum — сумма, которую пользователи оставили курьеру в качестве чаевых.
    courier_reward_sum — сумма, которую необходимо перечислить курьеру. Вычисляется как courier_order_sum + courier_tips_sum * 0.95 (5% — комиссия за обработку платежа).

 <!-- CDM -->

create table cdm.dm_courier_ledger(
    id int4 NOT NULL GENERATED ALWAYS AS IDENTITY, --идентификатор записи.
    courier_id int4 NOT null, 
    courier_name text NOT null, 
    settlement_year int4 NOT null,
    settlement_month int4 NOT null,
    orders_count int4 NOT null, 
    orders_total_sum numeric(19, 5) NOT null, 
    rate_avg numeric(19, 5) NOT null, 
    order_processing_fee numeric(19, 5) NOT null, -- orders_total_sum * 0.25.
    courier_order_sum numeric(19, 5) NOT null, 
    courier_tips_sum numeric(19, 5) NOT null,
    courier_reward_sum numeric(19, 5) NOT null --courier_order_sum + courier_tips_sum * 0.95 (5% — комиссия за обработку платежа).
    )


<!-- Список таблиц в слое DDS, из которых вы возьмёте поля для витрины. Отметьте, какие таблицы уже есть в хранилище, а каких пока нет. Недостающие таблицы вы создадите позднее. Укажите, как они будут называться. -->

/restaurants
_id — ID задачи;
name — название ресторана.

/couriers
_id — ID курьера в БД;
name — имя курьера.

/deliveries
order_id — ID заказа;
order_ts — дата и время создания заказа;
delivery_id — ID доставки;
courier_id —  ID курьера;
address — адрес доставки;
delivery_ts — дата и время совершения доставки;
rate — рейтинг доставки, который выставляет покупатель: целочисленное значение от 1 до 5;
tip_sum — сумма чаевых, которые оставил покупатель курьеру (в руб.)


    id — идентификатор записи.
    courier_id — couriers
    courier_name — Ф. И. О. курьера.
    settlement_year — /deliveries -> order_ts (year)
    settlement_month — /deliveries -> order_ts (month)
    orders_count — /deliveries
    orders_total_sum — /bonus transactions -> payment_sum
    rate_avg — /bonus transactions
    order_processing_fee — сумма, удержанная компанией за обработку заказов, которая высчитывается как orders_total_sum * 0.25.
    courier_order_sum —  /deliveries -> rate сумма, которую необходимо перечислить курьеру за доставленные им/ей заказы. За каждый доставленный заказ курьер должен получить некоторую сумму в зависимости от рейтинга (см. ниже).
    courier_tips_sum — /deliveries ->  tip_sum
    courier_reward_sum 
<!-- На основе списка таблиц в DDS составьте список сущностей и полей, которые необходимо загрузить из API. Использовать все методы API необязательно: важно загрузить ту информацию, которая нужна для выполнения задачи -->


drop table dds.restaurants;

create table dds.restaurants (
	id int4 NOT NULL GENERATED ALWAYS AS IDENTITY, --идентификатор записи.
	_id int4 NOT NULL, --идентификатор записи.
	name text not null
	);
drop table dds.couriers;

create table dds.couriers(
	id int4 NOT NULL GENERATED ALWAYS AS IDENTITY, 
	_id int4 NOT NULL,
	name text not null
	);

ALTER TABLE dds.couriers ADD PRIMARY KEY (id);
ALTER TABLE dds.couriers ADD CONSTRAINT dds_couriers_id UNIQUE (_id);

create table dds.deliveries(
	id int4 NOT NULL GENERATED ALWAYS AS IDENTITY, 
	order_id int4 NOT null,
	order_ts timestamp not null,
	delivery_id int4 NOT null,
	courier_id int4 NOT null, --foreign key to couriers
	address text not null,
	delivery_ts timestamp not null,
	rate int4 NOT NULL,
	tip_sum numeric(14,2) NOT null,
	FOREIGN KEY (courier_id) REFERENCES dds.couriers(_id) ON UPDATE cascade
	);

	
