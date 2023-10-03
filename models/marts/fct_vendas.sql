with
    dim_produtos as (
        select *
        from {{ ref('dim_produtos') }}
    )  
        
          , dim_clientes as (
        select *
        from {{ ref('dim_clientes') }}
    )  

          , pedido_itens as (
        select *
        from {{ ref('int_vendas_pedido_itens') }}
    )  

        , dim_funcionarios as (
            select *
            from {{ ref('dim_funcionarios') }}
    )

         , dim_fornecedores as (
            select *
            from {{ ref('dim_fornecedores') }}
    )


         , join_tabelas as (
        select
            pedidos.sk_pedido_item
            , pedidos.id_pedido
            , pedidos.id_funcionario
            , dim_clientes.pk_cliente
            , pedidos.id_transportadora
            , dim_produtos.pk_produto
            , pedidos.data_do_pedido
            , pedidos.frete
            , pedidos.destinatario
            , pedidos.endereco_destinatario
            , pedidos.cep_destinatario
            , pedidos.cidade_destinatario
            , pedidos.regiao_destinatario
            , pedidos.pais_destinatario
            , pedidos.data_do_envio
            , pedidos.data_requerida_entrega
            , pedidos.desconto_perc
            , pedidos.preco_da_unidade
            , pedidos.quantidade
            , dim_produtos.nome_produto
            , dim_produtos.nome_categoria
            , dim_produtos.nome_fornecedor
            , dim_produtos.is_discontinuado
            , dim_clientes.nome_cliente
            , dim_funcionarios.pk_funcionario
            , dim_funcionarios.ultimo_nome_funcionario
            , dim_funcionarios.primeiro_nome_funcionario
            , dim_funcionarios.cargo_funcionario
            , dim_funcionarios.pronome_funcionario
            , dim_funcionarios.data_nascimento_funcionario
            , dim_funcionarios.data_admissao_funcionario
            , dim_funcionarios.endereco_funcionario
            , dim_funcionarios.cidade_funcionario
            , dim_funcionarios.regiao_funcionario
            , dim_funcionarios.cep_funcionario
            , dim_funcionarios.pais_funcionario
            , dim_funcionarios.telefone_funcionario
            , dim_funcionarios.extencao_funcionario
            , dim_funcionarios.relatorios_funcionario
            , dim_funcionarios.descricao_regiao
            , dim_funcionarios.id_territorio
            , dim_funcionarios.descricao_territorio
            , dim_funcionarios.id_regiao
            , dim_fornecedores.pk_fornecedor
            , dim_fornecedores.id_produto
            , dim_fornecedores.id_fornecedor
            , dim_fornecedores.id_categoria
            , dim_fornecedores.quantidade_por_unidade
            , dim_fornecedores.preco_por_unidade
            , dim_fornecedores.unidades_em_estoque
            , dim_fornecedores.unidades_por_ordem
            , dim_fornecedores.nivel_reabastecimento
            , dim_fornecedores.contato_fornecedor
            , dim_fornecedores.contato_funcao
            , dim_fornecedores.endereco_fornecedor
            , dim_fornecedores.cidade_fornecedor
            , dim_fornecedores.regiao_fornecedor
            , dim_fornecedores.pais_fornecedor
            , dim_fornecedores.cep_fornecedor
        from pedido_itens as pedidos
        left join dim_produtos on
            pedidos.id_produto = dim_produtos.id_produto
        left join dim_clientes on
            pedidos.id_cliente = dim_clientes.id_cliente
       left join dim_funcionarios on  
            pedidos.id_funcionario = dim_funcionarios.id_funcionario
        left join dim_fornecedores on 
            dim_fornecedores.id_fornecedor = dim_produtos.id_fornecedor

    )
    
         , transformacoes as (
            select
            *
            , preco_da_unidade * quantidade as total_bruto
            , (1 - desconto_perc) * preco_da_unidade * quantidade as total_liquido
            , case
                when desconto_perc > 0 then true
                when desconto_perc = 0 then false
                else false
            end as is_desconto
            , frete / count(id_pedido) over(partition by id_pedido) as frete_ponderado
            from join_tabelas
    )
select *
from transformacoes