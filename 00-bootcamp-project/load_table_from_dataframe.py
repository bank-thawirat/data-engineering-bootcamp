# Ref: https://cloud.google.com/bigquery/docs/samples/bigquery-load-table-dataframe

import json
import os
from datetime import datetime

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account


# keyfile = os.environ.get("KEYFILE_PATH")
keyfile = 'data-engineer-384509-0bbebf9f91d9.json'
service_account_info = json.load(open(keyfile))
credentials = service_account.Credentials.from_service_account_info(service_account_info)
project_id = "data-engineer-384509"
client = bigquery.Client(
    project=project_id,
    credentials=credentials,
)

data = [
    {
        'file': './data/addresses.csv',
        'name': 'addresses',
        'parse_dates': [],
        'schema': [
            {
                'field': 'address_id',
                'type': 'string',
            },
            {
                'field': 'address',
                'type': 'string',
            },
            {
                'field': 'zipcode',
                'type': 'integer',
            },
            {
                'field': 'state',
                'type': 'string',
            },
            {
                'field': 'country',
                'type': 'string',
            }
        ],
        'partitioning': None,
        'clustering_fields': []
    },
    {
        'file': './data/events.csv',
        'name': 'events',
        'parse_dates': [],
        'schema': [
            {
                'field': 'event_id',
                'type': 'string',
            },
            {
                'field': 'session_id',
                'type': 'string',
            },
            {
                'field': 'page_url',
                'type': 'string',
            },
            {
                'field': 'created_at',
                'type': 'timestamp',
            },
            {
                'field': 'event_type',
                'type': 'string',
            },
            {
                'field': 'user',
                'type': 'string',
            },
            {
                'field': 'order',
                'type': 'string',
            },
            {
                'field': 'product',
                'type': 'string',
            }
        ],
        'partitioning': {
            'field': 'created_at',
            'type': 'day'
        },
        'clustering_fields': []
    },
    {
        'file': './data/order_items.csv',
        'name': 'order_items',
        'parse_dates': [],
        'schema': [
            {
                'field': 'order_id',
                'type': 'string',
            },
            {
                'field': 'product_id',
                'type': 'string',
            },
            {
                'field': 'quantity',
                'type': 'integer',
            }
        ],
        'partitioning': None,
        'clustering_fields': []
    },
    {
        'file': './data/orders.csv',
        'name': 'orders',
        'parse_dates': [],
        'schema': [
            {
                'field': 'order_id',
                'type': 'string',
            },
            {
                'field': 'created_at',
                'type': 'timestamp',
            },
            {
                'field': 'order_cost',
                'type': 'float',
            },
            {
                'field': 'shipping_cost',
                'type': 'float',
            },
            {
                'field': 'order_total',
                'type': 'float',
            },
            {
                'field': 'tracking_id',
                'type': 'string',
            },
            {
                'field': 'shipping_service',
                'type': 'string',
            },
            {
                'field': 'estimated_delivery_at',
                'type': 'string',
            },
            {
                'field': 'delivered_at',
                'type': 'timestamp',
            },
            {
                'field': 'status',
                'type': 'string',
            },
            {
                'field': 'user',
                'type': 'string',
            },
            {
                'field': 'promo',
                'type': 'string',
            },
            {
                'field': 'address',
                'type': 'string',
            }
        ],
        'partitioning': {
            'field': 'created_at',
            'type': 'day'
        },
        'clustering_fields': []
    },
    {
        'file': './data/products.csv',
        'name': 'products',
        'parse_dates': [],
        'schema': [
            {
                'field': 'product_id',
                'type': 'string',
            },
            {
                'field': 'name',
                'type': 'string',
            },
            {
                'field': 'price',
                'type': 'float',
            },
            {
                'field': 'inventory',
                'type': 'integer',
            }
        ],
        'partitioning': None,
        'clustering_fields': ['name']
    },
    {
        'file': './data/promos.csv',
        'name': 'promos',
        'parse_dates': [],
        'schema': [
            {
                'field': 'promo_id',
                'type': 'string',
            },
            {
                'field': 'discount',
                'type': 'float',
            },
            {
                'field': 'status',
                'type': 'string',
            }
        ],
        'partitioning': None,
        'clustering_fields': []
    },
    {
        'file': './data/users.csv',
        'name': 'users',
        'parse_dates': [],
        'schema': [
            {
                'field': 'user_id',
                'type': 'string',
            },
            {
                'field': 'first_name',
                'type': 'string',
            },
            {
                'field': 'last_name',
                'type': 'string',
            },
            {
                'field': 'email',
                'type': 'string',
            },
            {
                'field': 'phone_number',
                'type': 'string',
            },
            {
                'field': 'created_at',
                'type': 'timestamp',
            },
            {
                'field': 'updated_at',
                'type': 'timestamp',
            },
            {
                'field': 'address',
                'type': 'string',
            }
        ],
        'partitioning': {
            'field': 'created_at',
            'type': 'day'
        },
        'clustering_fields': ['first_name', 'last_name']
    }
]

for item in data:
    print(f"prepare date: {item['name']}")
    _parse_dates = []
    _schema = []
    _time_partitioning = None
    _clustering_fields = None
    for sche in item['schema']:
        if sche['type'] == 'timestamp':
            _parse_dates.append(sche['field'])
            _type = bigquery.SqlTypeNames.TIMESTAMP

        elif sche['type'] == 'integer':
            _type = bigquery.SqlTypeNames.INTEGER

        elif sche['type'] == 'float':
            _type = bigquery.SqlTypeNames.FLOAT
        
        else:
            _type = bigquery.SqlTypeNames.STRING

        _schema.append(bigquery.SchemaField(sche['field'], _type))

    if item['partitioning']:
        _time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=item['partitioning']['field'],
        )
    if len(item['clustering_fields']) > 0:
        _clustering_fields=item['clustering_fields']

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema = _schema,
        time_partitioning=_time_partitioning,
        clustering_fields=_clustering_fields,
    )

    file_path = item['file']
    df = pd.read_csv(file_path, parse_dates=_parse_dates)
    # df = pd.read_csv(file_path)
    df.info()

    table_id = f"{project_id}.deb_bootcamp.{item['name']}"
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    table = client.get_table(table_id)
    print(f"{item['name']} Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")