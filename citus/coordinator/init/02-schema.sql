-- eight shards for each sharded table
SET citus.shard_count = 8;

CREATE TABLE tenants
(
    tenant_id BIGINT PRIMARY KEY,
    name      TEXT NOT NULL
);

CREATE TABLE users
(
    tenant_id  BIGINT    NOT NULL,
    user_id    BIGINT    NOT NULL,
    email      TEXT      NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, user_id)
);

CREATE TABLE orders
(
    tenant_id  BIGINT         NOT NULL,
    order_id   BIGINT         NOT NULL,
    user_id    BIGINT         NOT NULL,
    amount     NUMERIC(12, 2) NOT NULL,
    status     TEXT           NOT NULL,
    created_at TIMESTAMP      NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, order_id)
);

CREATE TABLE order_items
(
    tenant_id BIGINT         NOT NULL,
    order_id  BIGINT         NOT NULL,
    line_no   INT            NOT NULL,
    sku       TEXT           NOT NULL,
    quantity  INT            NOT NULL,
    price     NUMERIC(12, 2) NOT NULL,
    PRIMARY KEY (tenant_id, order_id, line_no)
);

CREATE TABLE product_catalog
(
    sku          TEXT PRIMARY KEY,
    product_name TEXT NOT NULL,
    category     TEXT NOT NULL
);

-- shard tables by tenant_id
SELECT create_distributed_table('users', 'tenant_id');
SELECT create_distributed_table('orders', 'tenant_id');
SELECT create_distributed_table('order_items', 'tenant_id');

-- reference table replicated on all nodes
SELECT create_reference_table('product_catalog');
SELECT create_reference_table('tenants');
