# Проектная работа по DWH для нескольких источник

Задача — реализовать витрину для расчётов с курьерами. В ней вам необходимо рассчитать суммы оплаты каждому курьеру за предыдущий месяц. Например, в июне выплачиваем сумму за май. Расчётным числом является 10-е число каждого месяца (это не повлияет на вашу работу). Из витрины бизнес будет брать нужные данные в готовом виде.

Правила расчёта процента выплаты курьеру в зависимости от рейтинга, где r — это средний рейтинг курьера в расчётном месяце:
- r < 4 — 5% от заказа, но не менее 100 р.;
- 4 <= r < 4.5 — 7% от заказа, но не менее 150 р.;
- 4.5 <= r < 4.9 — 8% от заказа, но не менее 175 р.;
- 4.9 <= r — 10% от заказа, но не менее 200 р.

## CDM-слой
![Рисунок 1](/img/cdm_1.png)| 
|:--:| 
| *Рисунок 1 - Исходная диаграмма CDM-слоя* |

**Добавляется витрина cdm.dm_courier_ledger**
Состав витрины:
- id — идентификатор записи
- courier_id — ID курьера, которому перечисляем
- courier_name — Ф.И.О. курьера
- settlement_year — год отчёта
- settlement_month — месяц отчёта, где 1 — январь и 12 — декабрь
- orders_count — количество заказов за период (месяц)
- orders_total_sum — общая стоимость заказов
- rate_avg — средний рейтинг курьера по оценкам пользователей
- order_processing_fee — сумма, удержанная компанией за обработку заказов, которая высчитывается как orders_total_sum * 0.25
- courier_order_sum — сумма, которую необходимо перечислить курьеру за доставленные им/ей заказы. За каждый доставленный заказ курьер должен получить некоторую сумму в зависимости от рейтинга (см. ниже)
- courier_tips_sum — сумма, которую пользователи оставили курьеру в качестве чаевых
- courier_reward_sum — сумма, которую необходимо перечислить курьеру. Вычисляется как courier_order_sum + courier_tips_sum * 0.95 (5% — комиссия за обработку платежа)

![Рисунок 2](/img/cdm_2.png)| 
|:--:| 
| *Рисунок 2 - Итоговая диаграмма CDM-слоя* |

## DDS-слой
![Рисунок 3](/img/dds_1.png)| 
|:--:| 
| *Рисунок 3 - Исходная диаграмма DDS-слоя* |

**Добавляются две таблицы dm_couriers и dm_deliveries**
Состав таблицы dm_couriers:
- id - идентификатор курьера
- courier_id - ID курьера
- courier_name - Ф.И.О. курьера

Состав таблицы dm_deliveries:
- id - идентификатор доставки
- order_id - ID заказа
- delivery_id - ID доставки
- courier_id - идентификатор курьера
- address - адрес доставки
- delivery_ts_id - идентификатор доставки
- rate - оценка качества доставки
- order_sum - сумма доставки
- tip_sum - сумма чаевых

![Рисунок 4](/img/dds_2.png)| 
|:--:| 
| *Рисунок 4 - Итоговая диаграмма DDS-слоя* |

*P.S. Для улучшения можно было добавить внешний ключ для столбца "order_id" таблицы dm_deliveries на таблицу dm_orders, или наооборот добавить атрибут "delivery_id" в таблицу dm_orders и внешний ключ на таблицу dm_deliveries. Однако в проекте этого не делал, поскольку возникали сложности с внешними ключами при выполнении спринта из-за "живой" исходной БД.*


## STG-слой
![Рисунок 5](/img/stg_1.png)| 
|:--:| 
| *Рисунок 5 - Иходная диаграмма STG-слоя* |

**Добавляются две таблицы couriers и deliveries**

Состав таблицы couriers:
- id - Идентификатор курьера
- courier_id - ID курьера
- courier_name - Ф.И.О. курьера

Состав таблицы deliveries:
- id - идентификатор доставки
- order_id - ID заказа
- delivery_id - ID доставки
- courier_id - ID курьера
- address - адрес доставки
- delivery_ts - время доставки
- rate - оценка качества доставки
- sum - сумма доставки
- tip_sum - сумма чаевых

![Рисунок 6](/img/stg_2.png)| 
|:--:| 
| *Рисунок 6 - Итоговая диаграмма STG-слоя* |