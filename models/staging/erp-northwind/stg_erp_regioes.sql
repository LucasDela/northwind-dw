with
    fonte_regioes as (
        select cast(region_id as string) as id_regiao
            , cast(region_description as string) as descricao_regiao
        from {{ ref('region') }}
    )
select *
from fonte_regioes