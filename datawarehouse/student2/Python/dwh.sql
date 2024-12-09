-- Verwijder bestaande tabellen als ze al bestaan, rekening houdend met referentiële integriteit
DROP TABLE IF EXISTS fact_sales CASCADE;
DROP TABLE IF EXISTS dim_salesrep CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;



CREATE TABLE fact_sales (
                            fact_sales_SK SERIAL PRIMARY KEY,
                            date_SK INTEGER  NOT NULL REFERENCES dim_date(date_SK) ON DELETE CASCADE,  -- Verwijzing naar de `dim_date`-tabel
                            salesrep_SK INTEGER NOT NULL REFERENCES dim_salesrep(salesrep_SK) ON DELETE CASCADE,  -- Verwijzing naar de `dim_date`-tabel
                            amount DECIMAL(10, 2) NOT NULL,       -- Verkoopbedrag
                            quantity INT NOT NULL                 -- Aantal verkochte items
);
-- Maak de `dim_salesrep`-tabel aan
create table if not exists public.dim_salesrep
(
    salesrep_sk serial
        primary key,
    salesrepid  integer      not null,
    name        varchar(100) not null,
    office      varchar(100) not null,
    scd_start   timestamp    not null,
    scd_end     timestamp    not null,
    scd_version integer      not null,
    scd_active  boolean      not null
);

alter table public.dim_salesrep
    owner to postgres;

-- Maak de `dim_date`-tabel aan
create table if not exists public.dim_date
(
    date_sk      serial
        primary key,
    date         date,
    day_of_month smallint    not null,
    month        smallint    not null,
    year         smallint    not null,
    day_of_week  smallint    not null,
    day_of_year  smallint    not null,
    weekday      varchar(10) not null,
    month_name   varchar(15) not null,
    quarter      smallint    not null
);

alter table public.dim_date
    owner to postgres;
-- Maak de `fact_sales`-tabel aan
create table if not exists public.fact_sales
(
    fact_sales_sk serial
        primary key,
    date_sk       integer        not null
        references public.dim_date
            on delete cascade,
    salesrep_sk   integer        not null
        references public.dim_salesrep
            on delete cascade,
    revenue_mv    numeric(20, 2) not null,
    count_mv      integer        not null
);

alter table public.fact_sales
    owner to postgres;