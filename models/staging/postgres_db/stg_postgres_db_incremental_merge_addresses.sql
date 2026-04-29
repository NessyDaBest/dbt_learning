{{
    config(
        materialized = 'incremental',
        unique_key='address_id',
        incremental_strategy = 'merge'
    )
}}

with 

source as (

    select * from {{ source('postgres_db', 'addresses') }}

    {% if is_incremental() %}
        where _fivetran_synced > (select max(_fivetran_synced) from {{this}} )
    {% endif %}
),

renamed as (

    select
        address_id,
        zipcode,
        country,
        address,
        state,
        _fivetran_deleted,
        _fivetran_synced

    from source

)

select * from renamed