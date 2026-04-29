{{
    config(
        materialized='incremental',
        unique_key = 'user_id',
        incremental_strategy='delete+insert'
    )
}}


with 

source as (

    select * from {{ source('postgres_db', 'user') }}

    {% if is_incremental() %}
        where _fivetran_synced > (select max(_fivetran_synced) from {{this}})
    {% endif %}

),

renamed as (

    select
        user_id,
        updated_at,
        address_id,
        last_name,
        created_at,
        phone_number,
        total_orders,
        first_name,
        email,
        _fivetran_deleted,
        _fivetran_synced

    from source

)

select * from renamed