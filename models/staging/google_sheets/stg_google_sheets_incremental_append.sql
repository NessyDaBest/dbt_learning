{{
    config(
        materialized='incremental',
        incremental_strategy='append'
    )
}}


with 

source as (

    select * from {{ source('google_sheets', 'budget') }}

    {% if is_incremental() %}
        where _fivetran_synced > (select max(_fivetran_synced) from {{this}})
    {% endif %}
),

renamed as (

    select
        _row,
        quantity,
        month,
        product_id,
        _fivetran_synced

    from source

)

select * from renamed