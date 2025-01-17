with
    fonte_territorios as (
        select cast(territory_id as int) as id_territorio
            , cast(territory_description as string) as descricao_territorio
            , cast(region_id as int) as id_regiao
        from {{ ref('territories') }}
    )
select *
from fonte_territorios