with
    fonte_territorio_funcionarios as (
        select cast(employee_id as int) as id_funcionario
            , cast(territory_id as int) as id_territorio
        from {{ ref('employee_territories') }}
    )
select *
from fonte_territorio_funcionarios