with
    fonte_funcionarios as (
        select cast (employee_id as int) as id_funcionario
            , cast(last_name as string) as ultimo_nome_funcionario
            , cast(first_name as string) as primeiro_nome_funcionario
            , cast(title as string) as cargo_funcionario
            , cast(title_of_courtesy as string) as pronome_funcionario
            , cast(birth_date as date) as data_nascimento_funcionario
            , cast(hire_date as date) as data_admissao_funcionario
            , cast(address as string) as endereco_funcionario
            , cast(city as string) as cidade_funcionario
            , cast(region as string) as regiao_funcionario
            , cast(postal_code as string) as cep_funcionario
            , cast(country as string) as pais_funcionario
            , cast(home_phone as string) as telefone_funcionario
            , cast(extension as string) as extencao_funcionario
            , cast(reports_to as string) as relatorios_funcionario
        from {{ ref('employees') }}
    )
select *
from fonte_funcionarios