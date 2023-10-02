with   
     stg_regioes as ( 
        select *
        from {{ ref('stg_erp_regioes') }}
    )

    , stg_territorios as (
        select *
        from {{ ref('stg_erp_territorios') }}
    )
    , stg_territorio_funcionarios as (
            select* 
            from {{ ref('stg_erp_territorio_funcionarios') }}
    )
    , stg_funcionarios as (
            select* 
            from {{ ref('stg_erp_funcionarios') }}
    )


    , join_tabelas as (
        select  
            stg_funcionarios.id_funcionario
            , stg_funcionarios.ultimo_nome_funcionario
            , stg_funcionarios.primeiro_nome_funcionario
            , stg_funcionarios.cargo_funcionario
            , stg_funcionarios.pronome_funcionario
            , stg_funcionarios.data_nascimento_funcionario
            , stg_funcionarios.data_admissao_funcionario
            , stg_funcionarios.endereco_funcionario
            , stg_funcionarios.cidade_funcionario
            , stg_funcionarios.regiao_funcionario
            , stg_funcionarios.cep_funcionario
            , stg_funcionarios.pais_funcionario
            , stg_funcionarios.telefone_funcionario
            , stg_funcionarios.extencao_funcionario
            , stg_funcionarios.relatorios_funcionario
            , stg_regioes.descricao_regiao
            , stg_territorios.id_territorio
            , stg_territorios.descricao_territorio
            , stg_territorios.id_regiao
        from stg_funcionarios
        left join stg_territorio_funcionarios on
            stg_funcionarios.id_funcionario = stg_territorio_funcionarios.id_funcionario
        left join stg_territorios on    
            stg_territorio_funcionarios.id_territorio = stg_territorios.id_territorio
        left join stg_regioes on   
            stg_territorios.id_regiao = stg_regioes.id_regiao
    )

    , criar_chave as (
        select 
            row_number() over(order by id_funcionario) as pk_funcionario
            , *
        from join_tabelas
    )
    select * 
    from criar_chave